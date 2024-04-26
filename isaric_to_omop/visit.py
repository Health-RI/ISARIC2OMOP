import pandas as pd
from enum import Enum
from utils import increment_last_id
from concept import Concept, ISARICYesNo
import numpy as np

VISIT_OCCURENCE_HEADER = ["person_id", "visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date",
                          "visit_start_datetime", "visit_end_date", "visit_end_datetime", "visit_type_concept_id",
                          "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id",
                          "admitted_from_concept_id", "admitted_from_source_value", "discharged_to_concept_id",
                          "discharged_to_source_value", "preceding_visit_occurrence_id"]


class VisitConcept:
    inpatient_visit = 9201  # IP
    intensive_care = 32037  # OMOP4822460
    alternate_care_site = 33007  # OMOP5117447 (ACS)


visit_concept_type = Concept(concept_id=764466,
                             name="Electronic case report form",
                             dictionary="SNOMED",
                             domain="Type Concept",
                             concept_class="Record Artifact",
                             concept_code="455391000124107")


# {"Visit derived from EHR record": 44818518,
#                       "Electronic case report form": 764466}


def populate_icu_visits(icu_visits, postgres):
    icu_visits["visit_concept_id"] = VisitConcept.intensive_care
    icu_visits["visit_start_date"] = icu_visits["icu_hostdat"]
    icu_visits["visit_start_datetime"] = icu_visits["hosttim"]
    icu_visits["visit_end_date"] = pd.to_datetime(icu_visits["hoendat"])
    if pd.isnull(icu_visits["visit_end_date"]).any():
        print("warning")
        icu_visits = icu_visits.loc[pd.notnull(icu_visits["visit_end_date"])]
    icu_visits = icu_visits.reset_index(drop=True).drop_duplicates()
    increment_by_index = increment_last_id("visit_occurrence", "visit_occurrence_id", postgres)
    icu_visits.index += increment_by_index
    icu_visits.index.name = "visit_occurrence_id"
    icu_visits.reset_index(drop=False, inplace=True)
    icu_visits = icu_visits.reindex(columns=VISIT_OCCURENCE_HEADER)
    postgres.df_to_postgres(table="visit_occurrence", df=icu_visits)
    return icu_visits


def prepare_visit_occ(df, postgres):
    df = df[["person_id", "subjid", "sitename", "hostdat", "hosttim", "dsstdat", "hostdat_transfer", "icu_hoterm", 
             "icu_hostdat", "hoendat", "dsterm", "dsstdtc", "suppds_qval"]]
    df["dsstdat"] = pd.to_datetime(df["dsstdat"], errors="coerce")
    # , "hostdat", "hostdat_transfer", "icu_hostdat", "hoendat"
    df = df.sort_values(by=["subjid", "dsstdat"])
    df["hosttim"] = df["hosttim"].apply(lambda x: "00:00" if x == "-99" else x)
    df["hosttim"] = df.apply(lambda x: x["hostdat"] + " " + x["hosttim"] if pd.notnull(x["hosttim"]) else x["hosttim"], axis="columns")
    df.loc["hosttim"] = pd.to_datetime(df["hostdat"])
    df["visit_type_concept_id"] = visit_concept_type.concept_id
    print()
    icu_visits = df.loc[(df["icu_hoterm"] == ISARICYesNo.yes) & pd.notnull(df["icu_hostdat"])]

    icu_visits = populate_icu_visits(icu_visits, postgres)

    # other visit types

    # dsstdat is enrollment date

    return icu_visits
