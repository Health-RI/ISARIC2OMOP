import logging
import numpy as np
import pandas as pd

from utils import increment_last_id, GENERIC_COLUMNS
from concept import ISARICYesNo
from visit import visit_concept_type

log = logging.getLogger(__name__)


OMOP_PROCEDURE_COLUMNS = [
    "procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date", "procedure_datetime",
    "procedure_end_date", "procedure_end_datetime", "procedure_type_concept_id", "modifier_concept_id", "quantity",
    "provider_id", "visit_occurrence_id", "visit_detail_id", "procedure_source_value", "procedure_source_concept_id",
    "modifier_source_value"
]


class ProcedureConcept:
    """
    Assessment for admission to adult intensive care (SNOMED)
    https://athena.ohdsi.org/search-terms/terms/42539725
    """
    admission_to_icu = 42539725


class OMOPProcedure:
    daily_noninvasive_prtrt = 4177224
    daily_invasive_prtrt = 44790095
    daily_ecmo_prtrt = 4052536
    daily_nasaloxy_cmtrt = 37158406
    daily_inotrope_cmyn = 3655896
    daily_nitritc_cmtrt = 37154040
    daily_trach_prperf = 44783799
    daily_prone_cmtrt = 4196006
    oxygen_cmoccur = 4239130
    # oxygen_proccur = 4239130
    noninvasive_proccur = 4177224
    invasive_proccur = 44790095
    pronevent_prtrt = 4196006
    inhalednit_cmtrt = 37154040
    tracheo_prtrt = 44783799
    extracorp_prtrt = 4052536
    rrt_prtrt = 4146536
    antiviral_cmyn = 4140762
    antibiotic_cmyn = 4085730
    corticost_cmyn = 37312057
    antifung_cmyn = 36713613
    daily_neuro_cmtrt = 4084313
    other_cmyn = 0
    daily_other_prtrt = 0


def populate_icu_procedure(icu_visits, postgres):
    procedure_df = icu_visits.copy()
    procedure_df.rename(columns={"visit_start_date": "procedure_date",
                                 "visit_start_datetime": "procedure_datetime",
                                 "visit_end_date": "procedure_end_date",
                                 "visit_type_concept_id": "procedure_type_concept_id",
                                 "visit_end_datetime": "procedure_end_datetime"
                                 }, inplace=True)
    procedure_df["procedure_end_datetime"] = np.nan
    procedure_df.drop(
        columns=["visit_concept_id", "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id",
                 "admitted_from_concept_id", "admitted_from_source_value", "discharged_to_concept_id",
                 "discharged_to_source_value", "preceding_visit_occurrence_id"], inplace=True)
    procedure_df["procedure_concept_id"] = ProcedureConcept.admission_to_icu
    # todo propper incremental updates
    increment_by_index = increment_last_id("procedure_occurrence", "procedure_occurrence_id", postgres)
    procedure_df.index += increment_by_index
    procedure_df.index.name = "procedure_occurrence_id"
    procedure_df.reset_index(drop=False, inplace=True)
    postgres.df_to_postgres(table="procedure_occurrence", df=procedure_df)


def check_length(value):
    if len(str(value)) > 50:
        log.warning(f"Value {value} is too long and will be shortened")
        value = value[:50]
    return value


def populate_procedure(df, icu_visits, postgres):
    populate_icu_procedure(icu_visits, postgres)
    # xxx_cmyn/cmtrt fields are processed in drug_exposure.py
    procedure_columns = [x for x in df.columns if x.endswith("prtrt") or x.endswith("proccur")]
    # todo include xxx_prdur columns and calculate end date
    if "daily_prperf" in df.columns:
        columns = procedure_columns + GENERIC_COLUMNS + ["daily_prperf"]
    else:
        columns = procedure_columns + GENERIC_COLUMNS
    procedure_df = df.copy()[columns]
    if ("daily_prperf" in procedure_df.columns) and ("daily_other_prtrt" in procedure_df.columns):
        procedure_df.loc[procedure_df["daily_prperf"].apply(lambda x: x != ISARICYesNo.yes), "daily_other_prtrt"] = None

    procedure_df = pd.melt(procedure_df,
                           id_vars=GENERIC_COLUMNS,
                           value_vars=procedure_columns)
    procedure_df = procedure_df.loc[(procedure_df["value"] == ISARICYesNo.yes) |
                                    ((procedure_df["variable"] == "daily_other_prtrt") &
                                     (pd.notnull(procedure_df["value"])))
    ]
    procedure_df.loc[(procedure_df["variable"] == "daily_other_prtrt"), "procedure_source_value"] = procedure_df["value"]
    procedure_df["procedure_source_value"] = procedure_df["procedure_source_value"].apply(lambda x: check_length(x))

    # todo move to main for all dates columns
    procedure_df["dsstdat"] = pd.to_datetime(procedure_df["dsstdat"], errors="coerce")
    procedure_df["hostdat"] = pd.to_datetime(procedure_df["hostdat"], errors="coerce")
    procedure_df["procedure_date"] = procedure_df.apply(
        lambda x: x["hostdat"] if x["variable"].endswith("cmoccur") else x["dsstdat"], axis="columns")
    procedure_df["procedure_type_concept_id"] = visit_concept_type.concept_id
    procedure_df["procedure_concept_id"] = procedure_df["variable"].apply(
        lambda x: getattr(OMOPProcedure, x, None))
    if pd.isnull(procedure_df["procedure_concept_id"]).any():
        no_concept = procedure_df.loc[
            pd.isnull(procedure_df["procedure_concept_id"]), "variable"].unique().tolist()
        log.warning(f"Following ISARIC procedure values are not mapped to OMOP concepts: {', '.join(no_concept)}")
    procedure_df = procedure_df.loc[pd.notnull(procedure_df["procedure_concept_id"])]
    if pd.isnull(procedure_df["procedure_date"]).any():
        print("procedures with no dates will be excluded")
        procedure_df = procedure_df.loc[pd.notnull(procedure_df["procedure_date"])]
    increment_by_index = increment_last_id("procedure_occurrence", "procedure_occurrence_id", postgres)
    procedure_df.index += increment_by_index
    procedure_df.index.name = "procedure_occurrence_id"
    procedure_df.reset_index(drop=False, inplace=True)
    procedure_df = procedure_df.reindex(columns=OMOP_PROCEDURE_COLUMNS)
    postgres.df_to_postgres(table="procedure_occurrence", df=procedure_df)
