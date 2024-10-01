import datetime
import os
from os import getenv
from pathlib import Path
from time import sleep

import yaml
from etl.utils.parser import parse_config
from dotenv import load_dotenv

from etl.configs import Config
from etl.extractors.extractor import PostgresExtractor
from etl.loaders.loader import ElasticSaver
from etl.transformers.transformer import DataPrepare
from etl.utils.etl_state import State, JsonFileStorage

CONFIG_FILENAME = 'config.yaml'



def get_state_storage() -> State:
    """Настраиват контейнер для хранения состояний"""
    state_file_path = Path(__file__).parent.joinpath(config.etl.state_file_name)
    storage = JsonFileStorage(state_file_path)
    return State(storage)

def start_etl_process() -> None:
    """Запускает etl-процесс"""
    # logger.info('Скрипт начал работу...')
    elastic = ElasticSaver(config.etl.es)
    state = get_state_storage()
    postgres = PostgresExtractor(config.etl.postgres, state)
    transformer = DataPrepare()
    time_log_status = datetime.datetime.utcnow()
    while True:
        now = datetime.datetime.utcnow()
        data = postgres.load_data()
        if data:
            transformed_data = transformer.transform_data(data)

            elastic.save_to_es(transformed_data)
        if now > time_log_status + datetime.timedelta(seconds=config.etl.log_status_period):
            # logger.info('Скрипт работает в штатном режиме...')
            time_log_status = now
        sleep(config.etl.fetch_delay)


if __name__ == '__main__':
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    config_file = Path(__file__).parent.joinpath(CONFIG_FILENAME)
    config_dict = yaml.safe_load(config_file.open(encoding='utf-8'))
    # env_c = parse_config()
    print(config_dict)
    config = Config.parse_obj(config_dict)
    start_etl_process()