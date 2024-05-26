import numpy as np
import pandas as pd
from datetime import datetime
import logging

from utils import increment_last_id
from location import get_locations, populate_care_site
from core.db_connector import PostgresController
from typing import Dict, Union

from utils import merge_columns_with_postfixes
# from concept import OMOP_NO_MATCHING_CONCEPT

logger = logging.getLogger(__name__)


ISARIC_SEX_CODES = {"1": "Male", "2": "Female", "-1": "Not specified"}
OMOP_GENDER_CONCEPT = {"MALE": "8507", "FEMALE": "8532", "UNKNOWN": "8551"}

OMOP_PERSON_HEADER = [
        "person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
        "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
        "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
        "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"
    ]


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
    ethnicity_not_stated = 37394011  # non-standard deprecated concept


class OMOPRace:
    """"Definition of Race concepts based on concept ID"""
    arab = 38003616
    black = 38003598
    asian = 8515
    white = 8527
    native = 8557
    other_race = 8522  # non-standard deprecated concept
    unknown = 8552  # non-standard deprecated concept


class ISARICAgeUnits:
    years = 2
    months = 1


def solve_original_ethnic(value: Union[float, int], other_value: str):
    """
    Solves values for `ethnicity_source_value` converting ISARIC codes to human-readable
    :param value: float or int, value in ethnic column
    :param other_value: str, value in ethnic_other
    :return: str, human-readable ethnic value
    """
    ISARIC_ethnic = {1: "Arab", 2: "Black", 3: "East Asian", 4: "South Asian", 5: "West Asian", 6: "Latin American",
                     7: "White", 8: "Aboriginal/First Nations", 9: "Other", 10: "N/A"}

    ethnic_value = None
    if pd.notnull(value):
        ethnic_value = ISARIC_ethnic.get(int(value))
        if ethnic_value == "Other" and pd.notnull(other_value):
            ethnic_value = other_value
    if not ethnic_value:
        ethnic_value = "N/A"
    return ethnic_value


def map_omop_race(value):
    isaric_values_to_omop_race_concept = {"Arab": OMOPRace.arab,
                                          "Black": OMOPRace.black,
                                          "East Asian": OMOPRace.asian,
                                          "South Asian": OMOPRace.asian,
                                          "West Asian": OMOPRace.asian,
                                          "Latin American": OMOPRace.other_race,  # OMOP_NO_MATCHING_CONCEPT,
                                          "White": OMOPRace.white,
                                          "Aboriginal/First Nations": OMOPRace.native,
                                          "Other": OMOPRace.other_race,  # OMOP_NO_MATCHING_CONCEPT,
                                          "N/A": OMOPRace.unknown  # OMOP_NO_MATCHING_CONCEPT
                                          }

    race_concept_id = isaric_values_to_omop_race_concept.get(value)
    if not race_concept_id:
        logger.warning(f"{value} does not maps to vocabulary, will be set to 'Other' i.e. 8522")
        # logger.warning(f"{value} does not maps to vocabulary, will be set to 'No matching concept' i.e. 0")
        race_concept_id = OMOPRace.other_race  # OMOP_NO_MATCHING_CONCEPT
    return race_concept_id


def map_omop_ethnicity(value):
    """
    distinguishes only between 'Hispanic', 'Not Hispanic' and not specified
    """
    omop_ethnicity = OMOPEthnic.ethnicity_not_stated  # OMOP_NO_MATCHING_CONCEPT
    if pd.notnull(value):
        if int(value) == ISARICEthnic.latin_american:
            omop_ethnicity = OMOPEthnic.hispanic_or_latino
        elif int(value) in [ISARICEthnic.arab, ISARICEthnic.black, ISARICEthnic.white, ISARICEthnic.south_asian,
                            ISARICEthnic.aboriginal_first_nations, ISARICEthnic.west_asian, ISARICEthnic.east_asian]:
            omop_ethnicity = OMOPEthnic.not_hispanic_or_latino
    return omop_ethnicity


def prepare_person(data_df, postgres: PostgresController, location_ids_dict: Dict[str, int] = None):

    rename_dict = {"subjid": "person_source_value",
                   "sex": "gender_source_concept_id",
                   "sitename": "care_site_id",
                   "ethnic": "ethnicity_source_concept_id"}

    # Select data including 'ethnic___xx' columns
    person_source_columns = [
        "subjid", "sex", "age_estimateyears", "age_estimateyearsu", "ethnic", "other_ethnic", "country", "othcountry",
        "sitename", "dsstdat"
    ] + [column for column in data_df.columns if column.startswith("ethnic_")]

    df = data_df.copy()[person_source_columns]
    df = df.drop_duplicates(subset=["subjid"], keep="first")

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
    df["gender_source_value"] = df["gender_source_value"].apply(lambda x: ISARIC_SEX_CODES.get(x))
    df["gender_concept_id"] = df["gender_source_value"].apply(
        lambda x: OMOP_GENDER_CONCEPT.get(x.upper()) if pd.notnull(x) and x != "Not specified" else OMOP_GENDER_CONCEPT[
            "UNKNOWN"])
    # Solve ethnic and race
    for column in [x for x in df.columns if x.startswith("ethnic")]:
        df[column] = df[column].apply(lambda x: np.nan if x == "variable not in source" else x)
    df = merge_columns_with_postfixes(df=df, column_name="ethnic")
    df["ethnicity_source_value"] = df.apply(lambda x: solve_original_ethnic(x["ethnic"], x["other_ethnic"]),
                                            axis="columns")
    df["race_concept_id"] = df["ethnicity_source_value"].apply(lambda x: map_omop_race(x))
    df["ethnicity_concept_id"] = df["ethnic"].apply(lambda x: map_omop_ethnicity(x))

    # Prepare for DB
    df.rename(columns=rename_dict, inplace=True)
    df.reset_index(drop=True, inplace=True)
    increment_by_index = increment_last_id("person", "person_id", postgres)
    df.index += increment_by_index
    # todo: NB! No constraints in DB - check for uniqueness yourself
    df.index.name = "person_id"
    df.reset_index(drop=False, inplace=True)
    df = df.reindex(columns=OMOP_PERSON_HEADER)
    df["year_of_birth"] = df["year_of_birth"].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    # No data for birthdate time, setting to 1st of June
    df["birth_datetime"] = df["year_of_birth"].apply(lambda x: datetime(int(x), 6, 1, 0, 0, 0))
    # in care site id values like 468-0053 - can not be ingested because OMOP expect int
    care_sites_dict = populate_care_site(df, postgres)
    df["care_site_id"] = df["care_site_id"].apply(lambda x: care_sites_dict.get(str(x)) if pd.notnull(x) else x)
    postgres.df_to_postgres(table="person", df=df)
    return df
