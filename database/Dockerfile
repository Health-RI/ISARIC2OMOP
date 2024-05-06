FROM postgres:15.1

COPY init_db.sh /docker-entrypoint-initdb.d/
ADD vocabulary_download_v5.tar.gz . 
COPY OMOPCDM_postgresql_5.4_ddl.sql .
COPY OMOPCDM_postgresql_5.4_constraints.sql .
COPY OMOPCDM_postgresql_5.4_indices.sql .
COPY OMOPCDM_postgresql_5.4_primary_keys.sql .