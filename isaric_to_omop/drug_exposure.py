import logging
import pandas as pd
import re
from core.db_connector import PostgresController
from concept import ISARICYesNo
from typing import Union

from utils import GENERIC_COLUMNS, increment_last_id
from procedure import OMOP_PROCEDURE_COLUMNS
from visit import visit_concept_type


log = logging.getLogger(__name__)


OMOP_DRUG_EXP_FIELDS = [
    "drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date", "drug_exposure_start_datetime",
    "drug_exposure_end_date", "drug_exposure_end_datetime", "verbatim_end_date", "drug_type_concept_id", "stop_reason",
    "refills", "quantity", "days_supply", "sig", "route_concept_id", "lot_number", "provider_id", "visit_occurrence_id",
    "visit_detail_id", "drug_source_value", "drug_source_concept_id", "route_source_value", "dose_unit_source_value"
]


class ISARICProcedureTerms:
    ribavirin = 1
    lopinavir_ritonvir = 2
    Interferon_alpha = 3
    Interferon_beta = 4
    Neuraminidase_inhibitors = 5
    Other = 6


# class OMOPDrugs:
#     ribavirin = 1762711
#     Lopinavir_Ritonvir = 4275312
#     Interferon_alpha = 4333650
#     Interferon_beta = 46276542
#     Neuraminidase_inhibitors = 4333524


class OMOPDrugs:
    ribavirin = 4291865
    Lopinavir_Ritonvir = 4275312
    Interferon_alpha = 4333650
    Interferon_beta = 46276542
    Neuraminidase_inhibitors = 4333524


class OMOPTreatmentAsProcedure:
    daily_nasaloxy_cm = 37158406
    daily_inotrope_cm = 3655896
    daily_nitritc_cm = 37154040
    daily_prone_cm = 4196006
    inhalednit_cm = 37154040
    antiviral_cm = 4140762
    antibiotic_cm = 4085730
    corticost_cm = 37312057
    antifung_cm = 36713613
    daily_neuro_cm = 4084313
    daily_rrt_cm = 4146536
    inotrop_cm = 3655896
    other_cm = 0


def try_strip_postfix(value: str) -> Union[int, None]:
    value = "_".join(value.split("_")[:-1])
    return getattr(OMOPTreatmentAsProcedure, value, None)


def populate_procedure_treatment(df: pd.DataFrame, postgres: PostgresController) -> None:
    df = df.loc[(df["variable"].apply(lambda x: "anti" in x) & pd.notnull(df["avail"])) | 
                (df["value"] == ISARICYesNo.yes)
    ]
    df["procedure_date"] = df.apply(
        lambda x: x["start_date"] if pd.notnull(x["start_date"]) else x["dsstdat"], axis="columns")
    df["procedure_end_date"] = df.apply(
        lambda x: x["end_date"] if pd.notnull(x["end_date"]) else None, axis="columns")
    df["procedure_type_concept_id"] = visit_concept_type.concept_id
    df["procedure_concept_id"] = df["variable"].apply(lambda x: getattr(OMOPTreatmentAsProcedure, x, try_strip_postfix(x)))
    df.loc[df["value"].apply(lambda x: isinstance(x, str)), "procedure_source_value"] = df["value"]
    df = df.loc[pd.notnull(df["procedure_concept_id"])]
    if pd.isnull(df["procedure_date"]).any():
        print("procedures with no dates will be excluded")
        df = df.loc[pd.notnull(df["procedure_date"])]
    increment_by_index = increment_last_id("procedure_occurrence", "procedure_occurrence_id", postgres)
    df.index += increment_by_index
    df.index.name = "procedure_occurrence_id"
    df.reset_index(drop=False, inplace=True)
    df = df.reindex(columns=OMOP_PROCEDURE_COLUMNS)
    postgres.df_to_postgres(table="procedure_occurrence", df=df)


def populate_drug_exposure(df: pd.DataFrame, postgres: PostgresController) -> None:

    pattern = re.compile("([a-z0-9_]*_cm)(yn|trt|dat|route|end|type)(_(__)?[1-9]([0-9])?)?$")
    drug_columns = [x for x in df.columns if re.match(pattern, x)]
    drug_df = df.copy()[drug_columns + GENERIC_COLUMNS]

    core_drug_names = []
    for column in drug_columns:
        if re.match(r"[a-z0-9_]*_cmtrt(_[1-9]([0-9])?)?$", column):
            postfix = re.match(pattern, column).group(3) or ""
            core_name = re.match(pattern, column).group(1)
            name_n_postfix = core_name + postfix
            core_drug_names.append(name_n_postfix)
            drug_df[name_n_postfix] = drug_df.apply(
                lambda x: {"value": x[column],
                           "avail": x.get(f"{core_name}yn{postfix}", "no such column"),
                           "start_date": x.get(f"{core_name}dat{postfix}"),
                           "end_date": x.get(f"{core_name}end{postfix}"),
                           "route": x.get(f"{core_name}route{postfix}"),
                           "type": x.get(f"{core_name}type{postfix}")
                           },
                axis="columns")

    drug_df = pd.melt(drug_df, id_vars=GENERIC_COLUMNS, value_vars=core_drug_names)
    # unstack measurements columns and remove empty values
    values_df = pd.json_normalize(drug_df['value'])
    drug_df.drop(columns="value", inplace=True)
    drug_df = pd.merge(left=drug_df, right=values_df, left_index=True, right_index=True)
    drug_df = drug_df.loc[pd.notnull(drug_df["value"])]

    procedure_sub_df = drug_df.loc[drug_df["variable"].apply(
        lambda x: x in OMOPTreatmentAsProcedure.__dict__.keys() or "anti" in x or "other_cm" in x)]

    treatments_to_map = drug_df.loc[drug_df["value"].apply(lambda x: isinstance(x, str)) |
                                    drug_df["variable"].str.startswith("daily_dop")
    ]
    # for val in sorted(treatments_to_map["value"].apply(lambda x: x.lower()).unique().tolist()):
    #     print(val)

    leftover = drug_df.loc[~(drug_df.index.isin(procedure_sub_df.index) |
                             drug_df.index.isin(treatments_to_map.index)
                         )]

    if not leftover.empty:
        log.warning(f"Mapping for the following variables is not implemented: "
                    f"{', '.join(leftover['variable'].unique().tolist())}")

    populate_procedure_treatment(procedure_sub_df, postgres)
