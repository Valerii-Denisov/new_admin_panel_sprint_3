import datetime
import psycopg
from psycopg import ClientCursor, connection as pg_connection
from psycopg.rows import dict_row
from typing import Iterator, List, Tuple, Union
import sql

from etl.utils.backoff import backoff
from etl.utils.etl_state import State, JsonFileStorage
from etl.configs import (Config, DSNSettings, ESHost, ESSettings,
                         PostgresSettings)


class PostgresExtractor:
    """
    Класс для извлечения данных из PostgresQL
    """
    connection_params: DSNSettings
    connection: pg_connection
    state: State
    check_date: datetime.datatime
    limit: int

    def __init__(self, params: PostgresSettings, state: State):
        self.connection_params = params.dsn
        self.connection = self.get_connection()
        self.state = state
        self.check_date = self.state.get_state('last_update') or datetime.datetime.min
        self.limit = params.limit

    @backoff()
    def get_connection(self) -> pg_connection:
        with psycopg.connect(
                **self.connection_params.dict(),
                row_factory=dict_row(),
                cursor_factory=ClientCursor) as pg_conn:
            return pg_conn

    def executor(self, sql_query: str, params: tuple) -> Iterator[Tuple]:
        """Выполняет sql запрос"""
        while True:
            try:
                if self.connection.closed:
                    self.connection = self.get_connection()
                cursor = self.connection.cursor()
                cursor.execute(sql_query, params)
                while True:
                    chunk_data = cursor.fetchmany(self.limit)
                    if not chunk_data:
                        return
                    for row in chunk_data:
                        yield row
            except psycopg.OperationalError as pg_error:
                pass
        #logger.error('Ошибка соединения при выполнении SQL запроса %s', pg_error)

    @backoff(is_connection=False)
    def load_data(self) -> Union[Iterator[Tuple], List]:
        """
        Выгружает из postgres данные, обновленные после указанной даты:
        - находит новые записи и определяет id изменившихся фильмов
        - выгружает информацию для этих фильмов
        """
        now = datetime.datetime.utcnow()
        films_ids = self.executor(sql.movies_ids, (self.check_date,))
        films_ids = tuple([record[0] for record in films_ids])
        if films_ids:
            films_data = self.executor(sql.movies_data, (films_ids,))
            self.state.set_state('last_update', str(now))
            self.check_date = now
            return films_data
        return []
