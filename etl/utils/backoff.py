import random
import datetime as dt
from functools import wraps
from time import sleep

import elasticsearch
import psycopg


def get_sleep_time(
        start_sleep_time: float,
        border_sleep_time: float,
        factor: int,
        attempt_con: int,
        jitter: bool,
):
    try:
        if jitter:
            sleep_time = random.uniform(
                start_sleep_time,
                start_sleep_time * factor,
            ) * factor ** attempt_con
        else:
            sleep_time = start_sleep_time * factor ** attempt_con
    except OverflowError:
        sleep_time = border_sleep_time
    return min(border_sleep_time, sleep_time)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10,
            logger=None, is_connection=True, jitter=True):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attemp_con = 0
            begin = dt.datetime.now()
            while True:
                sleep_time = get_sleep_time(start_sleep_time, border_sleep_time, factor, attemp_con, jitter)
                try:
                    attemp_con += 1
                    sleep(sleep_time)
                    connection = func(*args, **kwargs)
                    finish_time = (dt.datetime.now() - begin).total_seconds()
                    if logger and is_connection:
                        logger.info('Attempts: %s. Time: %s seconds',
                                    attemp_con, finish_time)
                    return connection
                except psycopg.OperationalError:
                    if logger:
                        logger.error('Unable to connect to postgres')
                except elasticsearch.ConnectionError:
                    if logger:
                        logger.error('Unable to connect to elasticsearch')
        return inner
    return func_wrapper
