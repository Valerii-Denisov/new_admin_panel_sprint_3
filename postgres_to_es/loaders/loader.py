import json
import logging
from pathlib import Path
from typing import List

import elasticsearch
from elasticsearch import Elasticsearch, helpers

from configs import ESSettings, ESHost
from dataclass import Filmwork
from utils.backoff import backoff
from dataclasses import asdict

logger = logging.getLogger('app_logger')


class ElasticSaver:
    """Класс для работы с elasticsearch"""
    index_name: str
    host: ESHost
    index_config: str
    es_client: Elasticsearch

    def __init__(self, params: ESSettings):
        self.index_name = params.index_name
        self.host = params.default_host
        self.index_config = params.index_config
        self.es_client = self.get_es_client()
        if not self.check_index():
            self._create_index()

    def check_index(self) -> bool:
        """Проверяет существование индекса с указанным именем"""
        return self.es_client.indices.exists(index=self.index_name)

    @backoff()
    def get_es_client(self) -> Elasticsearch:
        """Создает клиент elasticsearch"""
        logger.info('Подключение к elasticsearch...')
        host = f'{self.host.host}:{self.host.port}'
        _client = Elasticsearch(hosts=host)
        _client.cluster.health(wait_for_status="yellow")
        logger.info('Соединение с elasticsearch успешно установлено!')
        return _client

    def _create_index(self) -> None:
        """Создает индекс с заданными настройками и названием"""
        path = Path(__file__).parent.joinpath(self.index_config)
        with open(path, 'r', encoding='utf-8') as index_file:
            index_settings = json.load(index_file)
        settings = index_settings.get('settings')
        mappings = index_settings.get('mappings')
        self.es_client.indices.create(index=self.index_name,
                                      settings=settings,
                                      mappings=mappings)

    def save_to_es(self, data: List[Filmwork]) -> None:
        """Загружает данные в индекс"""
        actions = [
            {
                "_index": "movies",
                "_id": movie.id,
                "_source": asdict(movie)
            }
            for movie in data
        ]
        while True:
            try:
                records, errors = helpers.bulk(client=self.es_client,
                                               actions=actions,
                                               raise_on_error=True,
                                               stats_only=True)
                if records:
                    logger.info('Фильмов обновлено в Elasticsearch: %s',
                                records)
                if errors:
                    logger.error(
                        'При обновлении Elasticsearch произошло ошибок: %s',
                        errors,
                    )
                return
            except elasticsearch.ConnectionError as error:
                logger.error(
                    'Отсутствует подключение к Elasticsearch %s',
                    error,
                )
                self.es_client = self.get_es_client()
