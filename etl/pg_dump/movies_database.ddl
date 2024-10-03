CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
    CONSTRAINT valid_rating CHECK (rating >= 0 and rating <= 100)
);

CREATE INDEX IF NOT EXISTS film_work_idx ON content.film_work (creation_date, rating);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE UNIQUE INDEX IF NOT EXISTS person_idx ON content.person (full_name);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE UNIQUE INDEX IF NOT EXISTS genre_idx ON content.genre (name, description);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    film_work_id uuid NOT NULL,
    person_id uuid NOT NULL,
    role TEXT NOT NULL,
    created timestamp with time zone
);

ALTER TABLE content.person_film_work ADD CONSTRAINT film_work_id_fk FOREIGN KEY (film_work_id) REFERENCES content.film_work(id);

ALTER TABLE content.person_film_work ADD CONSTRAINT person_id_fk FOREIGN KEY (person_id) REFERENCES content.person(id);

CREATE UNIQUE INDEX IF NOT EXISTS role_film_work_person_idx ON content.person_film_work (film_work_id, person_id, role);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL,
    film_work_id uuid NOT NULL,
    created timestamp with time zone
);

ALTER TABLE content.genre_film_work ADD CONSTRAINT genre_id_fk FOREIGN KEY (genre_id) REFERENCES content.genre(id);

ALTER TABLE content.genre_film_work ADD CONSTRAINT film_work_id_fk FOREIGN KEY (film_work_id) REFERENCES content.film_work(id);

CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre_idx ON content.genre_film_work (genre_id, film_work_id);
