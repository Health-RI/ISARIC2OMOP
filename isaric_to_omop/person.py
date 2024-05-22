import numpy as np
import pandas as pd
from datetime import datetime

from utils import increment_last_id
from location import get_locations, populate_care_site
from core.db_connector import PostgresController
from typing import Dict


class ISARICEthnic:
    arab = 1
    black = 2
    east_asian = 3
    south_asian = 4
    west_asian = 5
    latin_american = 6
    white = 7
    aboriginal_first_nations = 8
    other = 9
    na = 10


class OMOPEthnic:
    """"Definition of Ethnicity concepts based on concept ID"""
    hispanic_or_latino = 38003563
    not_hispanic_or_latino = 38003564
    # ethnicity_not_stated = 37394011 # non-standard depricated concept


class OMOPRace:
    """"Definition of Race concepts based on concept ID"""
    arab = 38003616
    black = 38003598
    asian = 8515
    white = 8527
    native = 8557
    # other_race = 8522 # non-standard depricated concept
    # unknown = 8552 # non-standard depricated concept


class ISARICAgeUnits:
    years = 2
    months = 1


def map_omop_ethnicity(value):
    isaric_values_to_omop_ethnicity_concept = {"Arab": OMOPEthnic.not_hispanic_or_latino,
                                          "Black": OMOPEthnic.not_hispanic_or_latino,
                                          "East Asian": OMOPEthnic.not_hispanic_or_latino,
                                          "South Asian": OMOPEthnic.not_hispanic_or_latino,
                                          "West Asian": OMOPEthnic.not_hispanic_or_latino,
                                          "Latin American": OMOPEthnic.hispanic_or_latino,
                                          "White": OMOPEthnic.not_hispanic_or_latino,
                                          "Aboriginal/First Nations": OMOPEthnic.not_hispanic_or_latino,
                                          "Other": 0,
                                          "N/A": 0}

    ethnic_concept_id = isaric_values_to_omop_ethnicity_concept.get(value)
    if not ethnic_concept_id:
        print(f"{value} does not map to vocabulary, will be set to 0")
        ethnic_concept_id = 0
    return ethnic_concept_id


def map_omop_race(value):
    isaric_values_to_omop_race_concept = {"Arab": OMOPRace.arab,
                                          "Black": OMOPRace.black,
                                          "East Asian": OMOPRace.asian,
                                          "South Asian": OMOPRace.asian,
                                          "West Asian": OMOPRace.asian,
                                          "Latin American": 0,
                                          "White": OMOPRace.white,
                                          "Aboriginal/First Nations": OMOPRace.native,
                                          "Other": 0,
                                          "N/A": 0}

    race_concept_id = isaric_values_to_omop_race_concept.get(value)
    if not race_concept_id:
        print(f"{value} does not maps to vocabulary, will be set to 0")
        race_concept_id = 0
    return race_concept_id


def prepare_person(df, postgres: PostgresController, location_ids_dict: Dict[str, int] = None):
    isaric_sex = {"1": "Male", "2": "Female", "-1": "Not specified"}
    OMOP_G_CONCEPT = {"MALE": "8507", "FEMALE": "8532", "UNKNOWN": "8551"}

    header = [
        "person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
        "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
        "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
        "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"
    ]
    rename_dict = {"subjid": "person_source_value",
                   "sex": "gender_source_concept_id",
                   "sitename": "care_site_id",
                   "ethnic": "ethnicity_source_concept_id"}
    # Solve year of birth
    df = df.loc[pd.notnull(df["age_estimateyears"])]
    df["age_estimateyears"] = \
        df.apply(lambda x: x["age_estimateyears"] / 12 if x["age_estimateyearsu"] == ISARICAgeUnits.months else x,
                 axis="columns")["age_estimateyears"]
    df["dsstdat"] = pd.to_datetime(df["dsstdat"], errors="coerce")
    df.loc[:, "year_of_birth"] = (df["dsstdat"].dt.year - df["age_estimateyears"]).round(0).astype(int).astype(str)
    # Solve location id
    if pd.notnull(df["country"]).any():
        if location_ids_dict is None:
            location_ids_dict = get_locations(postgres)
        df["location_id"] = df["country"].apply(lambda x: location_ids_dict.get(str(int(x))) if pd.notnull(x) else x)
    # Solve Gender
    df["sex"] = df["sex"].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    df["gender_source_value"] = df["sex"]
    df["gender_source_value"] = df["gender_source_value"].apply(lambda x: isaric_sex.get(x))
    df["gender_concept_id"] = df["gender_source_value"].apply(
        lambda x: OMOP_G_CONCEPT.get(x.upper()) if pd.notnull(x) and x != "Not specified" else OMOP_G_CONCEPT[
            "UNKNOWN"])
    # Solve ethnic
    df["ethnic"] = df["ethnic"].apply(lambda x: np.nan if x == "variable not in source" else x)
    df["ethnicity_concept_id"] = df["ethnicity_source_value"].apply(lambda x: map_omop_ethnicity(x))
    df["race_concept_id"] = df["ethnicity_source_value"].apply(lambda x: map_omop_race(x))

    # Prepare for DB
    df.rename(columns=rename_dict, inplace=True)
    df.reset_index(drop=True, inplace=True)
    increment_by_index = increment_last_id("person", "person_id", postgres)
    df.index += increment_by_index
    # todo: NB! No constraints in DB - check for uniqueness yourself
    df.index.name = "person_id"
    df.reset_index(drop=False, inplace=True)
    df = df.reindex(columns=header)
    df["year_of_birth"] = df["year_of_birth"].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    # No data for birthdate time, setting to 1st of June
    df["birth_datetime"] = df["year_of_birth"].apply(lambda x: datetime(int(x), 6, 1, 0, 0, 0))
    # in care site id values like 468-0053 - can not be ingested because OMOP expect int
    care_sites_dict = populate_care_site(df, postgres)
    df["care_site_id"] = df["care_site_id"].apply(lambda x: care_sites_dict.get(str(x)) if pd.notnull(x) else x)
    postgres.df_to_postgres(table="person", df=df)
    return df
