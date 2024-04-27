from concept import ISARICYesNo
import numpy as np
import pandas as pd

from enum import IntEnum
from utils import increment_last_id
from concept import ISARICYesNo
from visit import visit_concept_type


class ProcedureConcept:
    admission_to_icu = 4138933  # Procedure	SNOMED	Admission to intensive care unit	4138933	305351004


class ISARICProcedureTerms(IntEnum):
    ribavirin = 1
    lopinavir_ritonvir = 2
    Interferon_alpha = 3
    Interferon_beta = 4
    Neuraminidase_inhibitors = 5
    Other = 6


class OMOPDrugs:
    ribavirin = 4291865
    Lopinavir_Ritonvir = 4275312
    Interferon_alpha = 4333650
    Interferon_beta = 46276542
    Neuraminidase_inhibitors = 4333524


class ISARICColumnsToOMOP:
    daily_invasive_prtrt = 44790095
    daily_inotrope_cmyn = 3655896
    daily_neuro_cmtrt = 4084313
    daily_prone_cmtrt = 4050473
    oxygen_cmoccur = 4239130
    noninvasive_proccur = 4177224
    pronevent_prtrt = 4050473
    inhalednit_cmtrt = 37154040
    tracheo_prtrt = 44783799
    extracorp_prtrt = None
    rrt_prtrt = 4146536
    other_cmyn = None
    antiviral_cmyn = 40177878
    antibiotic_cmyn = 4085730
    corticost_cmyn = 4333495
    antifung_cmyn = 40177571


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
    # todo
    increment_by_index = increment_last_id("procedure_occurrence", "procedure_occurrence_id", postgres)
    procedure_df.index += increment_by_index
    procedure_df.index.name = "procedure_occurrence_id"
    procedure_df.reset_index(drop=False, inplace=True)
    postgres.df_to_postgres(table="procedure_occurrence", df=procedure_df)


def populate_procedure(df, icu_visits, postgres):
    populate_icu_procedure(icu_visits, postgres)

    procedure_columns = ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date",
                         "procedure_datetime", "procedure_end_date", "procedure_end_datetime",
                         "procedure_type_concept_id", "modifier_concept_id", "quantity", "provider_id",
                         "visit_occurrence_id", "visit_detail_id", "procedure_source_value",
                         "procedure_source_concept_id", "modifier_source_value"]
    
    procedure_df = df.copy()[["subjid", "person_id", "daily_invasive_prtrt", "daily_inotrope_cmyn", "daily_neuro_cmtrt",
                              "daily_prone_cmtrt", "oxygen_cmoccur", "noninvasive_proccur", "pronevent_prtrt",
                              "inhalednit_cmtrt", "tracheo_prtrt", "extracorp_prtrt", "rrt_prtrt", "other_cmyn",
                              "antiviral_cmyn", "antibiotic_cmyn", "corticost_cmyn", "antifung_cmyn",
                              "dsstdat", "hostdat"]]
    # antiviral_cmtrt
    
    yes_no_columns = ["daily_invasive_prtrt", "daily_inotrope_cmyn", "daily_neuro_cmtrt",
                              "daily_prone_cmtrt", "oxygen_cmoccur", "noninvasive_proccur", "pronevent_prtrt",
                              "inhalednit_cmtrt", "tracheo_prtrt", "extracorp_prtrt", "rrt_prtrt", "other_cmyn",
                              "antiviral_cmyn", "antibiotic_cmyn", "corticost_cmyn", "antifung_cmyn"]
    procedure_df = pd.melt(procedure_df,
                           id_vars=["subjid", "person_id", "dsstdat", "hostdat"],
                           value_vars=yes_no_columns)
    procedure_df = procedure_df.loc[procedure_df["value"] == ISARICYesNo.yes]
    procedure_df["procedure_date"] = procedure_df.apply(
        lambda x: x["hostdat"] if x["variable"].endswith("cmoccur") else x["dsstdat"], axis="columns")
    procedure_df["procedure_type_concept_id"] = visit_concept_type.concept_id
    procedure_df["procedure_concept_id"] = procedure_df["variable"].apply(
        lambda x: getattr(ISARICColumnsToOMOP, x))
    procedure_df = procedure_df.loc[pd.notnull(procedure_df["procedure_concept_id"])]
    increment_by_index = increment_last_id("procedure_occurrence", "procedure_occurrence_id", postgres)
    procedure_df.index += increment_by_index
    procedure_df.index.name = "procedure_occurrence_id"
    procedure_df.reset_index(drop=False, inplace=True)
    procedure_df = procedure_df.reindex(columns=procedure_columns)
    postgres.df_to_postgres(table="procedure_occurrence", df=procedure_df)
    
    print()

    pass
