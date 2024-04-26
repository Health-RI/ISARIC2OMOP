import numpy as np
import pandas as pd

from utils import increment_last_id
from location import populate_location



def solve_original_ethnic(value, other_value):
    ISARIC_ETH = {1: "Arab", 2: "Black", 3: "East Asian", 4: "South Asian", 5: "West Asian", 6: "Latin American",
                  7: "White", 8: "Aboriginal/First Nations", 9: "Other", 10: "N/A"}

    ethnic_value = None
    if pd.notnull(value):
        ethnic_value = ISARIC_ETH.get(value)
        if ethnic_value == "Other" and pd.notnull(other_value):
            ethnic_value = other_value
    if not ethnic_value:
        ethnic_value = "N/A"
    return ethnic_value


def map_omop_race(value):
    map = {"Arab": 38003616,
           "Black": 38003598,
           "East Asian": 8515,
           "South Asian": 8515,
           "West Asian": 8515,
           "White": 8527,
           # "Aboriginal/First Nations": 8522,  # map to other 
           "Other": 8522,
           "N/A": 8552}

    race_concept_id = map.get(value)
    if not race_concept_id:
        print(f"{value} does not maps to vocabulary, will be set to 'Other' i.e. 8522")
        race_concept_id = 8522
    return race_concept_id


def prepare_person(df, mapping, postgres):
    isaric_sex = {"1": "Male", "2": "Female", "-1": "Not specified"}
    OMOP_G_CONCEPT = {"MALE": "8507", "FEMALE": "8532", "UNKNOWN": "8551"}

    header = [
        "person_id", "gender_concept_id", "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime",
        "race_concept_id", "ethnicity_concept_id", "location_id", "provider_id", "care_site_id",
        "person_source_value", "gender_source_value", "gender_source_concept_id", "race_source_value",
        "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id"]
    rename_dict = {"subjid": "person_source_value",
                   "sex": "gender_source_concept_id",
                   "sitename": "care_site_id",
                   "ethnic": "ethnicity_source_concept_id"}
    # todo take codes from mapping
    df = df.loc[pd.notnull(df["age_estimateyears"])]
    df["age_estimateyears"] = \
    df.apply(lambda x: x["age_estimateyears"] / 12 if x["age_estimateyearsu"] == 1 else x, axis="columns")[
        "age_estimateyears"]
    df["dsstdat"] = pd.to_datetime(df["dsstdat"], errors="coerce")
    df.loc[:, "year_of_birth"] = (df["dsstdat"].dt.year - df["age_estimateyears"]).round(0).astype(int).astype(str)

    if pd.notnull(df["country"]).any():
        df = populate_location(person_df=df, postgres=postgres)

    df["ethnic"] = df["ethnic"].apply(lambda x: np.nan if x == "variable not in source" else x)

    df["sex"] = df["sex"].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    df["gender_source_value"] = df["sex"]
    df["gender_source_value"] = df["gender_source_value"].apply(lambda x: isaric_sex.get(x))
    df["gender_concept_id"] = df["gender_source_value"].apply(
        lambda x: OMOP_G_CONCEPT.get(x.upper()) if pd.notnull(x) and x != "Not specified" else OMOP_G_CONCEPT[
            "UNKNOWN"])
    # Solve ethnic

    map = {"Arab": 38003616,
           "Black": 38003598,
           "East Asian": 8515,
           "South Asian": 8515,
           "West Asian": 8515,
           "White": 8527,
           "Aboriginal/First Nations": 8522,  # map to other 
           "Other": 8522,
           "N/A": 8552}

    # ISARIC 6: "Latin American"
    omop_ethnic = {"Hispanic or Latino": 38003563, "Not Hispanic or Latino": 38003564}
    OMOP_UNKN = {"Other Race": 8522, "Unknown": 8552}

    df["ethnicity_source_value"] = df.apply(lambda x: solve_original_ethnic(x["ethnic"], x["other_ethnic"]),
                                            axis="columns")
    df["race_concept_id"] = df["ethnicity_source_value"].apply(lambda x: map_omop_race(x))
    df["ethnicity_concept_id"] = df["ethnic"].apply(
        lambda x: omop_ethnic["Hispanic or Latino"] if x == 6 else 37394011)  # Ethnicity not stated

    df.rename(columns=rename_dict, inplace=True)
    df.reset_index(drop=True, inplace=True)
    increment_by_index = increment_last_id("person", "person_id", postgres)
    df.index += increment_by_index
    # todo remove: NB! should be outoincrement by DB!
    df.index.name = "person_id"
    df.reset_index(drop=False, inplace=True)
    df = df.reindex(columns=header)
    # todo it is just for saving, remove 
    df["year_of_birth"] = df["year_of_birth"].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    # df.to_csv("./output/UMCG_person.csv")
    print()
    # in care site id values like 468-0053 - can not be ingested because OMOP expect int
    df["care_site_id"] = df["care_site_id"].apply(lambda x: int(str(x).split("-")[0]) if pd.notnull(x) else x)
    postgres.df_to_postgres(table="person", df=df)
    return df