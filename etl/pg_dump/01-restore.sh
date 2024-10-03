#!/bin/bash

set -e

file="/docker-entrypoint-initdb.d/dump.pgdata"

echo "Restoring DB using $file"
pg_restore -U $DB_USER --dbname=$DB_NAME --verbose --single-transaction < "$file" || exit 1