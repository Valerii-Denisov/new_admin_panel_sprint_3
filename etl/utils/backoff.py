import random

from functools import wraps
from time import sleep

def get_sleep_time(
        start_sleep_time: float,
        border_sleep_time: float,
        factor: int,
        attempt_con: int,
        jitter: bool,
):
    """Функция для расчета времени сна при наличии ошибки. """
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

def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, jitter=True):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания (border_sleep_time).

    :param jitter: Вносит фактор рандомного расчета времени
    :param start_sleep_time: Начальное время ожидания
    :param factor: Во сколько раз нужно увеличивать время ожидания на
    каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt_con = 1
            while True:
                try:
                    sleep_time = get_sleep_time(
                        start_sleep_time,
                        border_sleep_time,
                        factor,
                        attempt_con,
                        jitter
                    )
                    result = func(*args, **kwargs)
                except Exception as e:
                    print(e)
                    attempt_con +=1
                    sleep(sleep_time)
                else:
                    return result

        return inner

    return func_wrapper
