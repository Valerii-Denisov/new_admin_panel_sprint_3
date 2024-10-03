import datetime
import logging
import os

import psycopg
from psycopg import ClientCursor, connection as pg_connection
from psycopg.rows import dict_row
from typing import Iterator, List, Tuple, Union

from extractors import sql

from utils.backoff import backoff
from utils.etl_state import State
from configs import DSNSettings, PostgresSettings

logger = logging.getLogger('app_logger')


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
        self.check_date = (self.state.get_state('last_update')
                           or str(datetime.datetime.min))
        self.limit = int(os.environ.get('DB_LIMIT'))

    @backoff(start_sleep_time=1)
    def get_connection(self) -> pg_connection:
        logger.info('Подключение к Postgres...')
        connections = psycopg.connect(
            **self.connection_params.dict().get('dsn'),
            row_factory=dict_row,
            cursor_factory=ClientCursor,
        )
        logger.info('Соединение с Postgres успешно установлено!')
        return connections

    def executor(self, sql_query: str, params: tuple) -> Iterator[Tuple]:
        """Выполняет sql запрос"""
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(sql_query, params)
                while True:
                    chunk_data = cursor.fetchmany(self.limit)
                    if not chunk_data:
                        return
                    for row in chunk_data:
                        yield row
            except psycopg.OperationalError as pg_error:
                logger.info(f'ex:{pg_error}')

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
        logger.info(f'Выгружено {len(films_ids)} id фильмов.')
        if films_ids:
            films_data = self.executor(sql.movies_data, (films_ids,))
            self.state.set_state('last_update', str(now))
            self.check_date = now
            return films_data
        logger.info('Обновленные позиции отсутствуют.')
        return []
