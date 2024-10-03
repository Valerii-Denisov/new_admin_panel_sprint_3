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

ENTRYPOINT ["python", "./etl/main.py"]

FROM postgres:16 AS database

COPY ./pg_dump/dump.pgdata /docker-entrypoint-initdb.d/dump.pgdata
COPY ./pg_dump/01-restore.sh /docker-entrypoint-initdb.d/01-restore.sh
