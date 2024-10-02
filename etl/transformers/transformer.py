import logging

from etl.dataclass import FilmworkStorage, Filmwork, Person, Genre
from typing import Iterator, List, Tuple

logger = logging.getLogger('app_logger')


class DataPrepare:
    """
    Класс для преобразования данных из postgres в объекты класса Filmork и
    хранения их в контейнере для последующего сохранения в elasticsearch
    """
    inner_storage: FilmworkStorage

    def __init__(self):
        self.inner_storage = FilmworkStorage()

    def transform_data(self, data: Iterator[Tuple]) -> List[Filmwork]:
        """Преобразует данные из postgres в объекты класса Filmork """
        if self.inner_storage:
            self.inner_storage.clear()

        for movie in data:
            candidate = Filmwork(
                movie.get('id'),
                movie.get('title'),
                movie.get('description'),
                movie.get('imdb_rating'),
                movie.get('type'),
                movie.get('created'),
                movie.get('modified'),

            )
            person = Person(movie.get('person_id'), movie.get('full_name'))
            genre = Genre(movie.get('genre_id'), movie.get('name'))
            movie_string = self.inner_storage.get_or_append(candidate)
            movie_string.add_person(movie.get('role'), person)
            movie_string.add_genre(genre)
        logger.info('Подготовлено фильмов к обновлению в еlasticsearch: %s',
                    self.inner_storage.count())
        return self.inner_storage.get_all()
