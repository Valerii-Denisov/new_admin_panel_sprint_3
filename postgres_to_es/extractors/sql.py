movies_data = """
    SELECT DISTINCT fw.id AS id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        fw.type,
        fw.created,
        fw.modified,
        pfw.role,
        p.id AS person_id,
        p.full_name,
        g.id AS genre_id,
        g.name
    FROM content.film_work AS fw
    LEFT JOIN content.person_film_work AS pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person AS p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work AS gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre AS g ON g.id = gfw.genre_id
    WHERE fw.id = ANY(%s)
    ORDER BY fw.title;
"""

movies_ids = """
    SELECT DISTINCT fw.id
    FROM content.film_work AS fw
    LEFT JOIN content.person_film_work AS pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person AS p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work AS gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre AS g ON g.id = gfw.genre_id
    WHERE GREATEST(fw.modified, p.modified, g.modified) > %s
    ORDER BY fw.id;
"""
