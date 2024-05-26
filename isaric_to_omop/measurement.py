import pandas as pd
import re
from core.db_connector import PostgresController
from visit import visit_concept_type
from utils import prepare_autoincrement_index


OMOP_MEASUREMENT_HEADER = [
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
        "measurement_time", "measurement_type_concept_id", "operator_concept_id", "value_as_number",
        "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id",
        "visit_detail_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value",
        "unit_source_concept_id", "value_source_value", "measurement_event_id", "meas_event_field_concept_id"
    ]


class ISARIGlucoseCUnits:
    mmol_L = 1
    mg_dL = 2


class OMOPUnits:
    kg = 9529
    cm = 8582
    percent = 8554
    billion_cells_per_liter = 44777588  # 10^9 per liter
    billion_L = 9444  # 10^9 per liter
    thousand_uL = 8848  # 10^3 per microliter
    umol_L = 8749  # micromole per liter
    mmol_L = 8753  # millimole per liter
    mg_dL = 8840  # milligram per decilter
    celsius = 586323
    fahrenheit = 9289


class ISARICTemperatureUnits:
    celsius = 1
    fahrenheit = 2


class ISARICPressureUnits:
    kPa = 1
    mmHg = 2


class OMOPPressureUnits:
    kPa = 44777602
    mmHg = 8876


class OMOPMeasConcept:
    temp_vs = 4174894
    daily_temp_vs = 4174894
    hr_vs = 4239408
    daily_hr_vs = 4239408
    rr_vs = 4313591
    daily_rr_vs = 4313591
    sysbp_vs = 4152194
    diabp_vs = 4154790
    admission_diabp_vs = 4154790  # mmHg
    oxy_vs = 4011919
    daily_oxy_vs = 4011919
    height_vs = 4177340
    weight_vs = 4178502
    daily_fi02_lb = 4353936
    daily_sa02_lb = 40483579
    daily_pao2_lb = 37392673
    daily_pco2_lb = 37175110
    daily_ph_lb = 4099592
    daily_hco3_lb = 37398740
    avpu_vs = 3328462
    rass_vs = 36684829
    daily_gcs_lb = 40478949
    daily_gcs_vs = 40478949
    systolic_vs = 4152194
    diastolic_vs = 4154790
    daily_meanart_lb = 4298391
    daily_meanart_vs = 4298391
    daily_urine_lb = 4092643
    daily_hb_lb = 4094758
    daily_wbc_lb = 4298431
    daily_lymp_lb = 4214543
    daily_neutro_lb = 4148615
    daily_haematocrit_lb = 4267515
    daily_plt_lb = 4094890
    # daily_aptt_lb = 4175016
    daily_alt_lb = 4146380
    daily_bil_lb = 4230543
    daily_ast_lb = 4263457
    daily_glucose_lb = 4149519
    daily_bun_lb = 4094594
    daily_lactate_lb = 4133534
    daily_creat_lb = 4275203
    daily_sodium_lb = 4269839
    daily_potassium_lb = 4276440
    daily_procal_lb = 44791466
    daily_crp_lb = 4208414
    daily_ldh_lb = 4012918
    daily_ddimer_lb = 4322628
    daily_il6_lb = 4332015
    daily_stercap_vs = 3043332
    daily_baseex_lb = 44813260  # mmol/L
    daily_pt_lb = 4245261  # seconds
    daily_aptt_lb = 44809202
    daily_aptr_lb = 4307179
    daily_inr_lb = 4306239
    daily_crekin_lb = 4275203  # ???
    daily_ferritin_lb = 4176561  # ng/ml
    daily_fibrinogen_lb = 4094436
    stercap_vs = 3043332
    apvs_weight_vs = 4264825


def populate_measurements(df: pd.DataFrame, postgres: PostgresController):
    general_columns = ["daily_dsstdat", "person_id", "hostdat", "cestdat", "dsstdat"]
    # measurement column name pattern
    pattern = r"([a-z_]*_(vs|lb))(yn|orres(u)?)$"
    # select measurement columns dynamically
    measurement_columns = [x for x in df.columns if re.match(pattern, x)]

    measurements_df = df.copy()[general_columns + measurement_columns]

    # combine availability, result and units to a dictionary and melt the frame
    core_meas_names = []
    for column in measurement_columns:
        if column.endswith("orres"):
            core_name = re.match(pattern, column).group(1)
            core_meas_names.append(core_name)
            measurements_df[core_name] = measurements_df.apply(
                lambda x: {"value": x[column], "avail": x.get(f"{core_name}yn"), "unit": x.get(f"{column}u")}, axis="columns")

    measurements_df = pd.melt(measurements_df,
                              id_vars=general_columns,
                              value_vars=core_meas_names)
    # unstack measurements columns and remove empty values
    values_df = pd.json_normalize(measurements_df['value'])
    measurements_df.drop(columns="value", inplace=True)
    measurements_df = pd.merge(left=measurements_df, right=values_df, left_index=True, right_index=True)
    measurements_df = measurements_df.loc[pd.notnull(measurements_df["value"])]
    # todo filter on avail

    # todo code for units properly
    # {'daily_bil_lb', 'daily_bun_lb', 'daily_creat_lb', 'daily_ddimer_lb', 'daily_glucose_lb', 'daily_hb_lb', 
    # 'daily_lactate_lb', 'daily_lymp_lb', 'daily_neutro_lb', 'daily_plt_lb', 'daily_potassium_lb', 'daily_sodium_lb',
    # 'daily_wbc_lb', 'oxy_vs'}
    # measurements_df.loc[measurements_df["variable"] == "oxy_vsorres", "unit"] = OMOPUnits.percent
    # measurements_df.loc[measurements_df["variable"] == "height_vsorres", "unit"] = OMOPUnits.cm
    # measurements_df.loc[measurements_df["variable"] == "weight_vsorres", "unit"] = OMOPUnits.kg

    measurements_df["measurement_concept_id"] = measurements_df["variable"].apply(lambda x: getattr(OMOPMeasConcept, x))
    no_concept = measurements_df.loc[pd.isnull(measurements_df["measurement_concept_id"]), "variable"].values.tolist()
    if no_concept:
        print(no_concept)
    # todo dates
    measurements_df.rename(columns={"daily_dsstdat": "measurement_date",
                                    "value": "value_source_value",
                                    "unit": "concept_id"}, inplace=True)
    measurements_df["measurement_type_concept_id"] = visit_concept_type.concept_id

    measurements_df = measurements_df.loc[pd.notnull(measurements_df["measurement_concept_id"])]
    measurements_df = prepare_autoincrement_index(measurements_df,
                                                 "measurement",
                                                 "measurement_id",
                                                 postgres)
    measurement_df = measurements_df.reindex(columns=OMOP_MEASUREMENT_HEADER)
    postgres.df_to_postgres(table="measurement", df=measurement_df)
