import pandas as pd
import re
from concept import ISARICYesNo, OMOP_NO_MATCHING_CONCEPT
from utils import prepare_autoincrement_index, select_columns_by_pattern, GENERIC_COLUMNS
from core.db_connector import PostgresController
from visit import visit_concept_type

OMOP_OBSERVATIONS_HEADER = [
    "observation_id", "person_id", "observation_concept_id", "observation_date", "observation_datetime",
    "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id", "qualifier_concept_id",
    "unit_concept_id", "provider_id", "visit_occurrence_id", "visit_detail_id", "observation_source_value",
    "observation_source_concept_id", "unit_source_value", "qualifier_source_value", "value_source_value",
    "observation_event_id", "obs_event_field_concept_id"
]


class OMOPObservations:
    pregyn_rptestcd = 4299535
    smoking_mhyn_1 = 903654
    smoking_mhyn_2 = 903653
    smoking_mhyn_3 = 903651
    inabwalk_ceoccur_v2 = 4086548
    influ_mbyn = 37111241
    corna_mbcat = 4224007
    rsv_mbcat = 4274000
    adeno_mbcat = 4253647
    bact_mborres = 4165747
    # ardssev_ceterm_1 = 4116992  # 4195694 Acute Respiratory Distress Syndrome - condition
    # ardssev_ceterm_2 = 4285732
    # ardssev_ceterm_3 = 4087703  # 320651 https://athena.ohdsi.org/search-terms/terms/320651 - condition
    # ardssev_ceterm_4 = 44805604
 #   pneumothorax_ceterm = OMOP_NO_MATCHING_CONCEPT  # 38001046	Pneumothorax w CC; 38001045	Pneumothorax w MCC;
    # 38001047 Pneumothorax w/o CC/MCC
   # gastro_ceterm = OMOP_NO_MATCHING_CONCEPT  # 38001188	G.I. hemorrhage w CC; 38001187	G.I. hemorrhage w MCC
  #  liverdysfunction_ceterm = OMOP_NO_MATCHING_CONCEPT


class OMOPObservationSeverity:
    mild = 4116992
    moderate = 4285732
    severe = 4087703
    unknown = 44805604


class ISARICSeverity:
    mild = 1
    moderate = 2
    severe = 3
    unknown = 4


def select_and_process_microbiodata(df: pd.DataFrame) -> pd.DataFrame:
    # xx_mb(includes mbyn, mbcat, mborres, mbcatv)
    pattern = re.compile("[a-z_]*_mb(yn|orres|cat(y)?)(_[1-9])?$")
    mb_columns = [x for x in df.columns if re.match(pattern, x)]  # or x in ["smoking_mhyn", "mbperf"]]
    mb_df = select_columns_by_pattern(df=df, pattern=pattern, extra=["mbperf"])

    return mb_df

# def select_and_process_comorbidities(df: pd.DataFrame) -> pd.DataFrame:
    # select xxx_mhyn
    # return mhyn_df


def populate_observation(df: pd.DataFrame, postgres: PostgresController) -> None:
    # xx_mb(includes mbyn, mbcat, mborres, mbcatv)
    # measurement column name pattern
    pattern = re.compile("[a-z_]*_mb(yn|orres|cat(y)?)(_[1-9])?$")
    # select measurement columns dynamically
    observations_columns = [x for x in df.columns if re.match(pattern, x) or x in ["smoking_mhyn", "mbperf", "pregyn_rptestcd"]]

    observation_df = df.copy()[GENERIC_COLUMNS + observations_columns]

    observation_df = pd.melt(observation_df,
                             id_vars=GENERIC_COLUMNS,
                             value_vars=observations_columns)
    observation_df["observation_concept_id"] = observation_df["variable"].apply(
        lambda x: getattr(OMOPObservations, x, None))
    # todo a propper mapping
    observation_df.loc[(observation_df["variable"] == "smoking_mhyn") &
                       (observation_df["value"] == 1), "observation_concept_id"] = OMOPObservations.smoking_mhyn_1
    observation_df.loc[(observation_df["variable"] == "smoking_mhyn") &
                       (observation_df["value"] == 2), "observation_concept_id"] = OMOPObservations.smoking_mhyn_2
    observation_df.loc[(observation_df["variable"] == "smoking_mhyn") &
                       (observation_df["value"] == 3), "observation_concept_id"] = OMOPObservations.smoking_mhyn_3
    observation_df = observation_df.loc[(observation_df["variable"] == "smoking_mhyn") |
    (observation_df["value"] == ISARICYesNo.yes)]
    # todo move to main for all dates columns
    observation_df["daily_dsstdat"] = pd.to_datetime(observation_df["daily_dsstdat"], errors="coerce")
    observation_df["cestdat"] = pd.to_datetime(observation_df["cestdat"], errors="coerce")
    observation_df["observation_date"] = observation_df.apply(
        lambda x: x["cestdat"] if pd.notnull(x["cestdat"]) else x["daily_dsstdat"], axis="columns")
    if pd.isnull(observation_df["observation_date"]).any():
        print("observations with no dates will be excluded")
        observation_df = observation_df.loc[pd.notnull(observation_df["observation_date"])]
    observation_df["observation_type_concept_id"] = visit_concept_type.concept_id
    # todo add other observations, think if we can map visits etc
    observation_df = observation_df.loc[pd.notnull(observation_df["observation_concept_id"])]
    observation_df = prepare_autoincrement_index(observation_df,
                                                 "observation",
                                                 "observation_id",
                                                 postgres)
    observation_df = observation_df.reindex(columns=OMOP_OBSERVATIONS_HEADER)
    postgres.df_to_postgres(table="observation", df=observation_df)
