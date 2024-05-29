import logging

import pandas as pd
import re
from core.db_connector import PostgresController
from visit import visit_concept_type
from utils import prepare_autoincrement_index

from concept import ISARICYesNo, OMOP_NO_MATCHING_CONCEPT

log = logging.getLogger(__name__)

OMOP_MEASUREMENT_HEADER = [
    "measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
    "measurement_time", "measurement_type_concept_id", "operator_concept_id", "value_as_number",
    "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id",
    "visit_detail_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value",
    "unit_source_concept_id", "value_source_value", "measurement_event_id", "meas_event_field_concept_id"
]


class ISARIGlucoseCUnits:
    mmol_L = {"name": "mmol/L", "id": 1}
    mg_dL = {"name": "mg/dL", "id": 2}


class ISARISCreatinineUnits:
    umol_L = {"name": "umol/L", "id": 1}
    mg_dL = {"name": "mg/dL", "id": 2}


class ISARICHbConcentration:
    gram_per_L = {"name": "g/L", "id": 1}
    gram_per_dL = {"name": "g/dL", "id": 2}


class OMOPUnits:
    kg = {"name": "kg", "id": 9529}
    cm = {"name": "cm", "id": 8582}
    percent = {"name": "%", "id": 8554}
    # blood cell scounts
    billion_cells_per_liter = {"name": "x10^9/L", "id": 44777588}  # 10^9 per liter
    billion_L = {"name": "x10^9/L", "id": 9444}  # 10^9 per liter
    thousand_uL = {"name": "x10^3/uL", "id": 8848}  # 10^3 per microliter
    # concentrations
    umol_L = {"name": "umol/L", "id": 8749}  # micromole per liter
    mmol_L = {"name": "mmol/L", "id": 8753}  # millimole per liter
    mg_dL = {"name": "mg/dL", "id": 8840}  # milligram per decilter
    ng_ml = {"name": "ng/ml", "id": 8842}  # nanogram per milliliter
    gram_per_L = {"name": "g/L", "id": 8636}
    gram_per_dL = {"name": "g/dL", "id": 8713}
    milliequivalent_per_liter = {"name": "mEq/L", "id": 9557}
    cells_uL = {"name": "cells/uL", "id": 8784}  # cells per microliter
    unit_per_liter = {"name": "U/L", "id": 8645}
    # temperature
    celsius = {"name": "C", "id": 586323}
    fahrenheit = {"name": "F", "id": 9289}
    # pressure
    kPa = {"name": "kPa", "id": 44777602}
    mmHg = {"name": "mmHg", "id": 8876}
    # time
    second = {"name": "seconds", "id": 8555}
    # other
    ml_per_24h = {"name": "mL/24hours", "id": 8930}
    per_minute = {"name": "per minute", "id": 8541}
    mg_L = {"name": "mg/L", "id": 8751}


class ISARICCellCounts:
    billion_L = {"name": "", "id": 1}  # 10^9 per liter
    thousand_uL = {"name": "", "id": 2}  # 10^3 per microliter


class ISARICTemperatureUnits:
    celsius = {"name": "C", "id": 1}
    fahrenheit = {"name": "F", "id": 2}


class ISARICPressureUnits:
    kPa = {"name": "kPa", "id": 1}
    mmHg = {"name": "mmHg", "id": 2}


class ISARICDailyPTINRlbyn:
    pt = 1
    inr = 2


class ISARICAVPU:
    alert = 1
    verbal = 2
    pain = 3
    unresponsive = 4


class ISARICSternalCapillary:
    more_than_2_seconds = 1
    less_than_2_seconds = 2
    unknown = 3


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
    daily_ferritin_lb = 4176561  # ng/ml
    daily_fibrinogen_lb = 4094436
    stercap_vs = 3043332
    apvs_weight_vs = 4264825


MEAS_OMOP_UNITS_MAPPING = {
    "hr_vs": OMOPUnits.per_minute,
    "daily_hr_vs": OMOPUnits.per_minute,
    "rr_vs": OMOPUnits.per_minute,
    "daily_rr_vs": OMOPUnits.per_minute,
    "sysbp_vs": OMOPUnits.mmHg,
    "diabp_vs": OMOPUnits.mmHg,
    "admission_diabp_vs": OMOPUnits.mmHg,
    "oxy_vs": OMOPUnits.percent,
    "daily_oxy_vs": OMOPUnits.percent,
    "height_vs": {},
    "weight_vs": {},
    "daily_fi02_lb": OMOPUnits.percent,
    "daily_sao2_lb": OMOPUnits.percent,
    "daily_hco3_lb": OMOPUnits.milliequivalent_per_liter,
    "daily_baseex_lb": OMOPUnits.mmol_L,
    "systolic_vs": OMOPUnits.mmHg,
    "diastolic_vs": OMOPUnits.mmHg,
    "daily_meanart_vs": OMOPUnits.mmHg,
    "daily_urine_lb": OMOPUnits.ml_per_24h,
    "daily_lymp_lb": OMOPUnits.cells_uL,
    "daily_neutro_lb": OMOPUnits.cells_uL,
    "daily_haematocrit_lb": OMOPUnits.percent,
    "daily_pt_lb": OMOPUnits.second,
    "daily_alt_lb": OMOPUnits.unit_per_liter,
    "daily_ast_lb": OMOPUnits.unit_per_liter,
    "daily_meanart_lb": OMOPUnits.mmHg,
    "daily_sodium_lb": OMOPUnits.mmol_L,
    "daily_potassium_lb": OMOPUnits.mmol_L,
    "daily_procal_lb": OMOPUnits.ng_ml,
    "daily_crp_lb": OMOPUnits.mg_L,
    "daily_ldh_lb": OMOPUnits.unit_per_liter,
    "daily_ddimer_lb": OMOPUnits.mg_L,
    "daily_il6_lb": OMOPUnits.mg_L,
    "daily_ferritin_lb": OMOPUnits.ng_ml,
  #  "daily_fibrinogen_lb": , - not in http://capacity-covid.eu/wp-content/uploads/CAPACITY-REDCap-2.pdf
    "apvs_weight_vs": {}
}

MEAS_ISARIC_CODES_UNITS_MAPPING = {
    "temp_vs": ISARICTemperatureUnits,
    "daily_temp_vs": ISARICTemperatureUnits,
    "daily_pao2_lb": ISARICPressureUnits,
    "daily_pco2_lb": ISARICPressureUnits,
    "daily_bil_lb": ISARIGlucoseCUnits,
    "daily_hb_lb": ISARICHbConcentration,
    "daily_wbc_lb": ISARICCellCounts,
    "daily_plt_lb": ISARICCellCounts,
    "daily_glucose_lb": ISARIGlucoseCUnits,
    "daily_bun_lb": ISARIGlucoseCUnits,
    "daily_lactate_lb": ISARIGlucoseCUnits,
    "daily_creat_lb": ISARISCreatinineUnits,
}

UNITS_NOT_EXPECTED = [
    "daily_gcs_vs", "daily_inr_lb", "daily_ph_lb", "avpu_vs", "rass_vs", "daily_gcs_lb", "stercap_vs",
    "daily_aptt_lb", "daily_aptr_lb",
                      ]


def populate_measurements(df: pd.DataFrame, postgres: PostgresController):
    general_columns = ["daily_dsstdat", "person_id", "hostdat", "cestdat", "dsstdat", "daily_lbdat"]
    # measurement column name pattern
    pattern = re.compile("([a-z0-9_]*_(vs|lb))(yn|orres(u)?)$")
    # select measurement columns dynamically
    measurement_columns = [x for x in df.columns if re.match(pattern, x)]
    if "daily_lbperf" in df.columns:
        measurement_columns.append("daily_lbperf")

    measurements_df = df.copy()[general_columns + measurement_columns]
    # filter pt/inr before stacking columns
    # daily_pt_lborres - show the field ONLY if: [daily_pt_inr_lbyn] = '1'; daily_inr_lborres - show the field ONLY if:
    # [daily_pt_inr_lbyn] = '2' setting not matching the filter values to None to remove them afterward
    if "daily_pt_inr_lbyn" in df.columns:
        measurements_df.loc[measurements_df["daily_pt_inr_lbyn"].apply(
            lambda x: int(x) == ISARICDailyPTINRlbyn.pt if pd.notnull(x) else False), "daily_inr_lbyn"] = None
        measurements_df.loc[measurements_df["daily_pt_inr_lbyn"].apply(
            lambda x: int(x) == ISARICDailyPTINRlbyn.inr if pd.notnull(x) else False), "daily_pt_lbyn"] = None
        measurements_df.drop(columns=["daily_pt_inr_lbyn"], inplace=True)

    # combine availability, result and units to a dictionary and melt the frame
    core_meas_names = []
    for column in measurement_columns:
        if column.endswith("orres"):
            core_name = re.match(pattern, column).group(1)
            core_meas_names.append(core_name)
            measurements_df[core_name] = measurements_df.apply(
                lambda x: {"value": x[column],
                           "avail": x.get(f"{core_name}yn"),
                           "unit": x.get(f"{column}u"),
                           "daily_lbperf": x.get("daily_lbperf")
                           },
                axis="columns")

    measurements_df = pd.melt(measurements_df,
                              id_vars=general_columns,
                              value_vars=core_meas_names)
    # unstack measurements columns and remove empty values
    values_df = pd.json_normalize(measurements_df['value'])
    measurements_df.drop(columns="value", inplace=True)
    measurements_df = pd.merge(left=measurements_df, right=values_df, left_index=True, right_index=True)
    # todo remap PT/INR concepts properly

    # filter out laboratory data if daily_lbperf != 1 and measyrement xxx_lbyn != 1
    if pd.isnull(measurements_df["daily_lbperf"]).all() and pd.isnull(measurements_df["avail"]).all():
        logging.warning("Either 'daily_lbperf' is not in the data or empty and/or all xxx_vsyn and xxx_lbyn columns"
                        "are empty, measurements data will be ignored")
    # remove values based on availability filters
    # todo uncomment for real data:
    # measurements_df.loc[
    #     (measurements_df["variable"].str.endswith("_lb")) &
    #     ~(measurements_df["daily_lbperf"].apply(
    #         lambda x: int(x) == ISARICYesNo.yes if pd.notnull(x) else False)), "avail"] = None
    # check_availabitily = [
    #     re.match(pattern, c).group(1) for c in df.columns if c.endswith("_vsyn") or c.endswith("lbyn")]
    # measurements_df = measurements_df.loc[~(measurements_df["variable"].isin(check_availabitily)) |
    #                                       (measurements_df["avail"].apply(lambda x: int(x) == ISARICYesNo.yes if pd.notnull(x) else False))]

    measurements_df = measurements_df.loc[pd.notnull(measurements_df["value"])]
    # remap verbal values
    measurements_df["value"] = measurements_df.loc[measurements_df["variable"] == "avpu_vs", "value"].apply(
        lambda x: [key for key, value in ISARICAVPU.__dict__.items() if value == int(x)][0])
    measurements_df["value"] = measurements_df.loc[measurements_df["variable"] == "stercap_vs", "value"].apply(
        lambda x: [key.replace("_", " ").capitalize() for key, value in ISARICSternalCapillary.__dict__.items() if pd.notnull(x) and value == int(x)])
    measurements_df.loc[measurements_df["variable"] == "stercap_vs", "value"] = measurements_df["value"].apply(lambda x: x[0] if pd.notnull(x) else None) 

    # todo code for units properly
    # unit_concept_id
    # unit_source_value
    # unit_source_concept_id
    # measurements_df.loc[measurements_df["variable"] == "oxy_vsorres", "unit"] = OMOPUnits.percent
    # measurements_df.loc[measurements_df["variable"] == "height_vsorres", "unit"] = OMOPUnits.cm
    # measurements_df.loc[measurements_df["variable"] == "weight_vsorres", "unit"] = OMOPUnits.kg

    measurements_df["measurement_concept_id"] = measurements_df["variable"].apply(
        lambda x: getattr(OMOPMeasConcept, x, OMOP_NO_MATCHING_CONCEPT))
    no_concept = measurements_df.loc[
        measurements_df["measurement_concept_id"] == OMOP_NO_MATCHING_CONCEPT, "variable"].unique().tolist()
    if no_concept:
        log.warning(f"No matching concept for the following measurements: {', '.join(no_concept)}")
    # Set dates
    measurements_df.loc[
        measurements_df["variable"].str.endswith("_lb"), "measurement_date"] = pd.to_datetime(measurements_df["daily_lbdat"], errors="coerce")
    measurements_df.loc[
        pd.isnull(measurements_df["measurement_date"]), "measurement_date"] = pd.to_datetime(measurements_df["daily_dsstdat"], errors="coerce")
    measurements_df.loc[
        pd.isnull(measurements_df["measurement_date"]), "measurement_date"] = pd.to_datetime(measurements_df["hostdat"], errors="coerce")
    measurements_df.loc[
        pd.isnull(measurements_df["measurement_date"]), "measurement_date"] = pd.to_datetime(measurements_df["dsstdat"], errors="coerce")
    if pd.isnull(measurements_df["measurement_date"]).any():
        log.warning("measurements with no dates will be excluded")
        measurements_df = measurements_df.loc[pd.notnull(measurements_df["measurement_date"])]
    measurements_df.rename(columns={
        "value": "value_source_value",
        "unit": "unit_source_value"}, inplace=True)
    measurements_df["measurement_type_concept_id"] = visit_concept_type.concept_id

    measurements_df = measurements_df.loc[pd.notnull(measurements_df["measurement_concept_id"])]
    measurements_df = prepare_autoincrement_index(measurements_df,
                                                  "measurement",
                                                  "measurement_id",
                                                  postgres)
    measurement_df = measurements_df.reindex(columns=OMOP_MEASUREMENT_HEADER)
    postgres.df_to_postgres(table="measurement", df=measurement_df)
