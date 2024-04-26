#!/bin/sh

psql -d omop_db -U postgres -c "CREATE SCHEMA omop;"
psql -d omop_db -U postgres -f ../OMOPCDM_postgresql_5.4_ddl.sql
psql -d omop_db -U postgres -c "\copy omop.concept FROM '../CONCEPT.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.concept_ancestor FROM '../CONCEPT_ANCESTOR.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.concept_class FROM '../CONCEPT_CLASS.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.concept_relationship FROM '../CONCEPT_RELATIONSHIP.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.concept_synonym FROM '../CONCEPT_SYNONYM.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.domain FROM '../DOMAIN.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.drug_strength FROM '../DRUG_STRENGTH.csv' WITH (DELIMITER E'\t', FORMAT TEXT, NULL '', HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.relationship FROM '../RELATIONSHIP.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -c "\copy omop.vocabulary FROM '../VOCABULARY.csv' WITH (DELIMITER E'\t', FORMAT TEXT, HEADER TRUE);"
psql -d omop_db -U postgres -f ../OMOPCDM_postgresql_5.4_primary_keys.sql
psql -d omop_db -U postgres -f ../OMOPCDM_postgresql_5.4_constraints.sql
psql -d omop_db -U postgres -f ../OMOPCDM_postgresql_5.4_indices.sql