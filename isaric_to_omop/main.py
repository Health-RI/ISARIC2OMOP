from person import prepare_person
from visit import prepare_visit_occ
import numpy as np
import os
import pandas as pd
from core.db_connector import PostgresController
from procedure import populate_procedure
from location import populate_location
from condition import populate_condition_occurence
from observation import populate_observation
from measurement import populate_measurements
from drug_exposure import populate_drug_exposure


def process_input(path, postgres):
    data_df = pd.read_csv(path, sep=",")
    if "ethnic" not in data_df.columns:
        data_df["ethnic"] = np.nan
    location_ids_dict = None
    if pd.notnull(data_df["country"]).any():
        location_df = data_df.copy()[["country"]].dropna(axis="rows").drop_duplicates()
        location_ids_dict = populate_location(location_df=location_df, postgres=postgres)
    # Prepare Person
    person_df = prepare_person(data_df, postgres, location_ids_dict)
    person_ids_dict = pd.Series(person_df["person_id"].values, index=person_df["person_source_value"]).to_dict()
    # Add OMOP DB ids to input df
    data_df = data_df.loc[data_df["subjid"].isin(person_ids_dict.keys())]
    data_df["person_id"] = data_df["subjid"].apply(lambda x: person_ids_dict[x])
    # Prepare and populate visit
    visit_occurrence_df = data_df.copy()
    icu_visits = prepare_visit_occ(visit_occurrence_df, postgres)
    populate_condition_occurence(df=data_df, postgres=postgres)
    populate_observation(df=data_df, postgres=postgres)
    populate_measurements(df=data_df, postgres=postgres)
    # Prepare and populate procedure
    populate_procedure(df=data_df, icu_visits=icu_visits, postgres=postgres)
    populate_drug_exposure(df=data_df, postgres=postgres)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # todo: a proper parameter for import
    path1 = "./NFU/data/Erasmus/scrambled.csv"
    path2 = "./NFU/data/AMC/scrambled.csv"
    path3 = "./NFU/data/Radboud/scrambled.csv"
    path4 = "./NFU/data/UMCG/scrambled.csv"

    postgres = PostgresController(db_name=os.environ["POSTGRES_DB"],
                                  postgres_connection=os.environ["OMOP_DB_CONNECT"],
                                  cdm_schema=os.environ["CDM_SCHEMA"],
                                  vocab_schema=os.environ["VOCAB_SCHEMA"])
    process_input(path4, postgres)
