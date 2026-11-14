#!/bin/bash
# Se ejecuta UNA vez, cuando el PVC está vacío (initdb). Crea usuarios de mínimo privilegio
# con contraseñas que llegan del Secret; nada sensible queda en la imagen ni en Git.
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username postgres --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE bi_reader  NOLOGIN;
CREATE ROLE etl_writer NOLOGIN;
CREATE ROLE ${SOLARBI_ETL_USER}  LOGIN PASSWORD '${SOLARBI_ETL_PASSWORD}' IN ROLE etl_writer;
CREATE ROLE ${SOLARBI_PG_READER_USER} LOGIN PASSWORD '${SOLARBI_PG_READER_PASSWORD}' IN ROLE bi_reader
  CONNECTION LIMIT 20;
ALTER DATABASE "${POSTGRES_DB}" OWNER TO ${SOLARBI_ETL_USER};
ALTER SCHEMA public OWNER TO ${SOLARBI_ETL_USER};
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
