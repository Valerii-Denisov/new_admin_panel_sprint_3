FROM python:3.10 AS service

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt requirements.txt

RUN  apt-get update \
     && apt-get install nano \
     && apt update && apt upgrade -y\
     && apt install netcat-traditional \
     && pip install --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "./postgres_to_es/main.py"]

FROM postgres:16 AS database

COPY ./postgres_to_es/pg_dump/movies_database.ddl /docker-entrypoint-initdb.d/01-movies_database.sql
COPY ./postgres_to_es/pg_dump/inserts.sql /docker-entrypoint-initdb.d/02-inserts.sql
