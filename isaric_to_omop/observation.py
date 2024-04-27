import pandas as pd
from concept import ISARICYesNo
from utils import prepare_autoincrement_index
from core.db_connector import PostgresController
from visit import visit_concept_type


class OMOPObservations:
    pneumothorax_ceterm = 253796
    pleuraleff_ceterm = 254061
    cryptogenic_ceterm = 36714118
    bronchio_ceterm = 4165112
    gastro_ceterm = 192671
    pancreat_ceterm = 4192640
    liverdysfunction_ceterm = 4245975
    hyperglycemia_aeterm = 4214376
    hypoglycemia_ceterm = 24609


def populate_observation(df: pd.DataFrame, postgres: PostgresController) -> None:
    observations_header = ["observation_id", "person_id", "observation_concept_id", "observation_date",
                           "observation_datetime", "observation_type_concept_id", "value_as_number", "value_as_string",
                           "value_as_concept_id",
                           "qualifier_concept_id", "unit_concept_id", "provider_id", "visit_occurrence_id",
                           "visit_detail_id", "observation_source_value", "observation_source_concept_id",
                           "unit_source_value", "qualifier_source_value", "value_source_value", "observation_event_id",
                           "obs_event_field_concept_id"]

    observations_columns = ["bactpneu_ceterm", "ards_ceterm", "ardssev_ceterm", "pneumothorax_ceterm",
                            "pleuraleff_ceterm", "cryptogenic_ceterm", "bronchio_ceterm", "meningitis_ceterm",
                            "seizure_ceterm", "heartfailure_ceterm", "arrhythmia_ceterm", "ischaemia_ceterm",
                            "cardiacarrest_ceterm", "bacteraemia_ceterm", "coagulo_ceterm", "rhabdomyolsis_ceterm",
                            "gastro_ceterm", "pancreat_ceterm", "liverdysfunction_ceterm", "hyperglycemia_aeterm",
                            "hypoglycemia_ceterm"]

    general_columns = ["daily_dsstdat", "person_id", "hostdat", "cestdat"]

    observation_yes_no_columns = ["pneumothorax_ceterm", "pleuraleff_ceterm", "cryptogenic_ceterm", "bronchio_ceterm",
                                  "gastro_ceterm", "pancreat_ceterm", "liverdysfunction_ceterm", "hyperglycemia_aeterm",
                                  "hypoglycemia_ceterm"]

    observation_df = df.copy()[general_columns + observations_columns]
    observation_df = pd.melt(observation_df,
                             id_vars=general_columns,
                             value_vars=observation_yes_no_columns)
    # todo clarify about daily_dssdat
    observation_df = observation_df.loc[observation_df["value"] == ISARICYesNo.yes]
    observation_df["observation_date"] = observation_df.apply(
        lambda x: x["cestdat"] if pd.notnull(x["cestdat"]) else x["daily_dsstdat"], axis="columns")
    if pd.isnull(observation_df["observation_date"]).any():
        print("observations with no dates will be excluded")
        observation_df = observation_df.loc[pd.notnull(observation_df["observation_date"])]
    observation_df["observation_type_concept_id"] = visit_concept_type.concept_id
    observation_df["observation_concept_id"] = observation_df["variable"].apply(
        lambda x: getattr(OMOPObservations, x))
    # todo add other observations, think if we can map visits etc
    observation_df = observation_df.loc[pd.notnull(observation_df["observation_concept_id"])]
    observation_df = prepare_autoincrement_index(observation_df,
                                               "observation",
                                               "observation_id",
                                               postgres)
    observation_df = observation_df.reindex(columns=observations_header)
    postgres.df_to_postgres(table="observation", df=observation_df)