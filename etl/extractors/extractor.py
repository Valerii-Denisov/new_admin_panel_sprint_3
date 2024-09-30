import datetime
from pathlib import Path

import psycopg
from psycopg import ClientCursor, connection as pg_connection
from psycopg.rows import dict_row
from typing import Iterator, List, Tuple, Union

from pydantic.experimental.pipeline import transform

import sql

from etl.utils.backoff import backoff
from etl.utils.etl_state import State, JsonFileStorage
from etl.configs import (Config, DSNSettings, ESHost, ESSettings,
                         PostgresSettings)
from etl.transformers.transformer import DataPrepare

class PostgresExtractor:
    """
    Класс для извлечения данных из PostgresQL
    """
    connection_params: DSNSettings
    connection: pg_connection
    state: State
    check_date: datetime.datetime
    limit: int

    def __init__(self, params: PostgresSettings, state: State):
        self.connection_params = params
        self.state = state
        self.check_date = self.state.get_state('last_update') or datetime.datetime.min
        self.limit = 100

    @backoff(start_sleep_time=1)
    def get_connection(self) -> pg_connection:
            return psycopg.connect(
                **self.connection_params,
                row_factory=dict_row,
                cursor_factory=ClientCursor,
        )

    def executor(self, sql_query: str, params: tuple) -> Iterator[Tuple]:
        """Выполняет sql запрос"""
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                sql = cursor.mogrify(sql_query, params)
                print(sql)
                cursor.execute(sql)
                while True:
                    chunk_data = cursor.fetchmany(self.limit)
                    if not chunk_data:
                        return
                    for row in chunk_data:
                        yield row
            except psycopg.OperationalError as pg_error:
                print(f'ex:{pg_error}')


    @backoff()
    def load_data(self) -> Union[Iterator[Tuple], List]:
        """
        Выгружает из postgres данные, обновленные после указанной даты:
        - находит новые записи и определяет id изменившихся фильмов
        - выгружает информацию для этих фильмов
        """
        films_ids = self.executor(sql.movies_ids, (self.check_date,))
        now = datetime.datetime.utcnow()
        films_ids = [(record['id']) for record in films_ids]
        print('films', len(films_ids))
        if films_ids:
            films_data = self.executor(sql.movies_data, (films_ids,))
            self.state.set_state('last_update', str(now))
            self.check_date = now
            return films_data
        return []

dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': '123qwe',
        'host': '0.0.0.0',
        'port':  5432,
        'options': '-c search_path=content',
        # 'limit': 100
    }

state_file_path = Path(__file__).parent.joinpath('state.json')
storage = JsonFileStorage(state_file_path)
postgres = PostgresExtractor(dsl, State(storage))
data = postgres.load_data()
transformer = DataPrepare()
trans_data = transformer.transform_data(data)
print(trans_data)
# len_ = [print(data_) for data_ in data]
# print(len(len_))
