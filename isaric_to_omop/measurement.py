import pandas as pd
from core.db_connector import PostgresController
from visit import visit_concept_type
from utils import prepare_autoincrement_index


class ISARICUnits:
    mmol_L = 1
    mg_dL = 2


class OMOPUnits:
    mmol_L = 8753
    mg_dL = 8840
    kg = 9529
    cm = 8582
    percent = 8554


class ISARICTemperatureUnits:
    celsius = 1
    fahrenheit = 2


class OMOPTemperatureUnits:
    celsius = 586323
    fahrenheit = 9289
    

class OMOPMeasConcept:
    temp_vsyn = 4302666
    hr_vsyn = 4239408
    rr_vsyn = 4313591
    sysbp_vsyn = 4152194
    diabp_vsyn = 4154790
    oxy_vsorres = 4020553
    height_vsorres = 4177340
    weight_vsorres = 4099154
    avpu_vsorres = 3328462
    rass_vsyn = 36684829
    daily_gcs_lbyn = 40478949
    systolic_vsyn = 4152194
    diastolic_vsyn = 4154790
    daily_urine_lbyn = 4092643
    daily_hb_lbyn = 4094758


def populate_measurements(df: pd.DataFrame, postgres: PostgresController):
    measurement_columns = [
        "temp_vsyn", "temp_vsorresu", "hr_vsyn", "rr_vsyn", "sysbp_vsyn", "diabp_vsyn", "oxy_vsorres", "height_vsorres",
        "weight_vsorres", "corna_mbcaty", "daily_fi02_lbyn", "daily_sa02_lbyn", "daily_pao2_lbyn",
        "daily_pao2_lborresu", "daily_pco2_lbyn", "daily_pco2_lborresu", "daily_ph_lbyn", "daily_hco3_lbyn",
        "avpu_vsorres", "rass_vsyn", "daily_gcs_lbyn", "systolic_vsyn", "diastolic_vsyn", "daily_urine_lbyn",
        "daily_hb_lbyn", "daily_hb_lborresu", "daily_wbc_lbyn", "daily_wbc_lborresu", "daily_lymp_lbyn",
        "daily_neutro_lbyn", "daily_haematocrit_lbyn", "daily_plt_lbyn", "daily_plt_lborresu", "daily_aptt_lbyn",
        "daily_pt_inr_lbyn", "daily_alt_lbyn", "daily_bil_lbyn", "daily_bil_lborresu", "daily_ast_lbyn",
        "daily_glucose_lbyn", "daily_glucose_lborresu", "daily_bun_lbyn", "daily_bun_lborresu", "daily_lactate_lbyn",
        "daily_lactate_lborresu", "daily_creat_lbyn", "daily_creat_lborresu", "daily_sodium_lbyn",
        "daily_potassium_lbyn", "daily_procal_lbyn", "daily_crp_lbyn", "daily_ldh_lbyn", "daily_ddimer_lbyn",
        "daily_ferritin_lbyn", "daily_il6_lbyn"
    ]

    measurement_header = [
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
        "measurement_time", "measurement_type_concept_id", "operator_concept_id", "value_as_number",
        "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id",
        "visit_detail_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value",
        "unit_source_concept_id", "value_source_value", "measurement_event_id", "meas_event_field_concept_id"
    ]

    general_columns = ["daily_dsstdat", "person_id", "hostdat", "cestdat"]

    measurements_df = df.copy()[general_columns + measurement_columns]

    value_columns = [x for x in measurement_columns if x.endswith("orres")]
    new_meas_names = []
    for column in value_columns:
        new_name = column + "_new"
        new_meas_names.append(new_name)
        measurements_df[new_name] = measurements_df.apply(
            lambda x: {"value": x[column], "unit": x.get(f"{new_name}orresu")}, axis="columns")

    measurements_df = pd.melt(measurements_df,
                              id_vars=general_columns,
                              value_vars=new_meas_names)
    measurements_df["variable"] = measurements_df["variable"].apply(lambda x: x.replace("_new", ""))
    values_df = pd.json_normalize(measurements_df['value'])
    measurements_df.drop(columns="value", inplace=True)
    measurements_df = pd.merge(left=measurements_df, right=values_df, left_index=True, right_index=True)
    measurements_df.loc[measurements_df["variable"] == "oxy_vsorres", "unit"] = OMOPUnits.percent
    measurements_df.loc[measurements_df["variable"] == "height_vsorres", "unit"] = OMOPUnits.cm
    measurements_df.loc[measurements_df["variable"] == "weight_vsorres", "unit"] = OMOPUnits.kg

    measurements_df = measurements_df.loc[pd.notnull(measurements_df["value"])]

    measurements_df["measurement_concept_id"] = measurements_df["variable"].apply(lambda x: getattr(OMOPMeasConcept, x))
    measurements_df.rename(columns={"daily_dsstdat": "measurement_date",
                                    "value": "value_source_value",
                                    "unit": "concept_id"}, inplace=True)
    measurements_df["measurement_type_concept_id"] = visit_concept_type.concept_id

    measurements_df = measurements_df.loc[pd.notnull(measurements_df["measurement_concept_id"])]
    measurements_df = prepare_autoincrement_index(measurements_df,
                                                 "measurement",
                                                 "measurement_id",
                                                 postgres)
    measurement_df = measurements_df.reindex(columns=measurement_header)
    postgres.df_to_postgres(table="measurement", df=measurement_df)
    print()



