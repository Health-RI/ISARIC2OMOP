#!/bin/sh

set -e

psql -d $POSTGRES_DB -U $POSTGRES_USER -c "CREATE SCHEMA $CDM_SCHEMA;"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "CREATE SCHEMA $VOCAB_SCHEMA;"
psql -d $POSTGRES_DB -U $POSTGRES_USER -f ../OMOPCDM_postgresql_5.4_ddl.sql -v CDM_SCHEMA=$CDM_SCHEMA -v VOCAB_SCHEMA=$VOCAB_SCHEMA
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.concept FROM '../vocabulary_download_v5/CONCEPT.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.concept_ancestor FROM '../vocabulary_download_v5/CONCEPT_ANCESTOR.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.concept_class FROM '../vocabulary_download_v5/CONCEPT_CLASS.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.concept_relationship FROM '../vocabulary_download_v5/CONCEPT_RELATIONSHIP.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.concept_synonym FROM '../vocabulary_download_v5/CONCEPT_SYNONYM.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.domain FROM '../vocabulary_download_v5/DOMAIN.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.drug_strength FROM '../vocabulary_download_v5/DRUG_STRENGTH.csv' WITH (DELIMITER E'\t', FORMAT TEXT, NULL '', HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.relationship FROM '../vocabulary_download_v5/RELATIONSHIP.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -c "\copy $VOCAB_SCHEMA.vocabulary FROM '../vocabulary_download_v5/VOCABULARY.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d $POSTGRES_DB -U $POSTGRES_USER -f ../OMOPCDM_postgresql_5.4_primary_keys.sql -v CDM_SCHEMA=$CDM_SCHEMA -v VOCAB_SCHEMA=$VOCAB_SCHEMA
psql -d $POSTGRES_DB -U $POSTGRES_USER -f ../OMOPCDM_postgresql_5.4_constraints.sql -v CDM_SCHEMA=$CDM_SCHEMA -v VOCAB_SCHEMA=$VOCAB_SCHEMA
psql -d $POSTGRES_DB -U $POSTGRES_USER -f ../OMOPCDM_postgresql_5.4_indices.sql -v CDM_SCHEMA=$CDM_SCHEMA -v VOCAB_SCHEMA=$VOCAB_SCHEMA