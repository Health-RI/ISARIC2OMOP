FROM postgres:15.1

COPY init_db.sh /docker-entrypoint-initdb.d/
COPY vocabulary_download_v5/* .
COPY OMOPCDM_postgresql_5.4_ddl.sql .
COPY OMOPCDM_postgresql_5.4_constraints.sql .
COPY OMOPCDM_postgresql_5.4_indices.sql .
COPY OMOPCDM_postgresql_5.4_primary_keys.sql .