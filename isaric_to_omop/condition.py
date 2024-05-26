import pandas as pd
from utils import prepare_autoincrement_index
from visit import visit_concept_type
from core.db_connector import PostgresController

from concept import ISARICYesNo


OMOP_CONDITION_HEADER = ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date",
                         "condition_start_datetime", "condition_end_date", "condition_end_datetime",
                         "condition_type_concept_id", "condition_status_concept_id", "stop_reason", "provider_id",
                         "visit_occurrence_id", "visit_detail_id", "condition_source_value",
                         "condition_source_concept_id", "condition_status_source_value"]


class OMOPConditions:
    chroniccard_mhyn = 4134586
    chronicpul_mhyn = 316866
    asthma_mhyn = 317009
    renal_mhyn = 46271022
    chronicneu_mhyn = 4134145
    malignantneo_mhyn = 443392
    aidshiv_mhyn = 4267417  # AIDS; should be mapped to 4013106 HIV positive as well
    obesity_mhyn = 433736
    diabetes_mhyn = 201820
    dementia_mhyn = 4182210
    malnutrition_mhyn = 4156515
    dehydration_vsorres = 42873066
    cough_ceoccur_v2 = 4038519
    coughsput_ceoccur_v2 = 4102774
    coughhb_ceoccur_v2 = 261687
    sorethroat_ceoccur_v2 = 4147326
    runnynose_ceoccur_v2 = 4276172
    earpain_ceoccur_v2 = 4103476
    wheeze_ceoccur_v2 = 314754
    chestpain_ceoccur_v2 = 77670
    myalgia_ceoccur_v2 = 442752
    jointpain_ceoccur_v2 = 77074
    shortbreath_ceoccur_v2 = 312437
    headache_ceoccur_v2 = 378253
    confusion_ceoccur_v2 = 4164633
    seizures_cecoccur_v2 = 377091
    abdopain_ceoccur_v2 = 200219
    vomit_ceoccur_v2 = 27674
    diarrhoea_ceoccur_v2 = 196523
    conjunct_ceoccur_v2 = 379019
    rash_ceoccur_v2 = 140214
    skinulcers_ceoccur_v2 = 4262920
    lymp_ceoccur_v2 = 315085
    bleed_ceoccur_v2 = 437312
    bactpneu_ceterm = 257315
    ards_ceterm = 4195694
    pneumothorax_ceterm = 253796
    pleuraleff_ceterm = 254061
    cryptogenic_ceterm = 36714118
    bronchio_ceterm = 4165112
    meningitis_ceterm = 435785  # meningitis; should be mapped to 378143 Encephalitis as well
    seizure_ceterm = 377091
    stroke_ceterm = 381316
    heartfailure_ceterm = 319835
    arrhythmia_ceterm = 44784217
    ischaemia_ceterm = 4186397
    cardiacarrest_ceterm = 321042
    bacteraemia_ceterm = 132736
    coagulo_ceterm = 432585  # Blood coagulation disorder; should be mapped to 436093 Disseminated intravascular coagulation as well
    rhabdomyolsis_ceterm = 4345578  # Rhabdomyolysis; should be mapped to 73001 myositis as well
    gastro_ceterm = 192671
    pancreat_ceterm = 4192640
    liverdysfunction_ceterm = 4245975
    hyperglycemia_aeterm = 4214376
    hypoglycemia_ceterm = 24609


def populate_condition_occurence(df: pd.DataFrame, postgres: PostgresController) -> None:

    cond_occ_columns = ["chroniccard_mhyn", "chronicpul_mhyn", "asthma_mhyn", "renal_mhyn", "modliver_mhyn", "mildliv_mhyn",
                         "chronicneu_mhyn", "malignantneo_mhyn", "chronhaemo_mhyn", "aidshiv_mhyn",
                         "obesity_mhyn", "diabetiscomp_mhyn", "diabetes_mhyn", "rheumatology_mhyr",
                         "dementia_mhyn", "malnutrition_mhyn", "smoking_mhyn", "dehydration_vsorres",
                         "fever_ceoccur_v2", "cough_ceoccur_v2", "coughsput_ceoccur_v2", "coughhb_ceoccur_v2",
                         "sorethroat_ceoccur_v2", "runnynose_ceoccur_v2", "earpain_ceoccur_v2",
                         "wheeze_ceoccur_v2", "chestpain_ceoccur_v2", "myalgia_ceoccur_v2",
                         "jointpain_ceoccur_v2", "fatigue_ceoccur_v2", "shortbreath_ceoccur_v2",
                         "headache_ceoccur_v2", "inablewalk_ceoccur_v2", "confusion_ceoccur_v2",
                         "seizures_cecoccur_v2", "abdopain_ceoccur_v2", "vomit_ceoccur_v2",
                         "diarrhoea_ceoccur_v2", "conjunct_ceoccur_v2", "rash_ceoccur_v2",
                         "skinulcers_ceoccur_v2", "lymp_ceoccur_v2", "bleed_ceoccur_v2", "bleed_ceterm_v2", "bleed_cetermy_v2",
                         "viralpneu_ceterm", "bactpneu_ceterm", "ards_ceterm", "ardssev_ceterm",
                         "pneumothorax_ceterm", "pleuraleff_ceterm", "cryptogenic_ceterm", "bronchio_ceterm",
                         "meningitis_ceterm", "seizure_ceterm", "stroke_ceterm", "heartfailure_ceterm",
                         "arrhythmia_ceterm", "ischaemia_ceterm", "cardiacarrest_ceterm", "bacteraemia_ceterm",
                         "coagulo_ceterm", "aneamia_ceterm", "rhabdomyolsis_ceterm", "renalinjury_ceterm",
                         "gastro_ceterm", "pancreat_ceterm", "liverdysfunction_ceterm", "hyperglycemia_aeterm",
                         "hypoglycemia_ceterm", "other_ceoccur", "other_ceterm"]

    general_columns = ["dsstdat", "person_id", "hostdat", "cestdat"]

    # 'flw_loss_smell' 'flw2_loss_smell' 'flw_loss_taste' 'flw2_loss_taste' "losssmell_ceoccur_v2",
    #                          "losstaste_ceoccur_v2"
    condition_df = df.copy()[general_columns + cond_occ_columns]
    condition_df = pd.melt(condition_df,
                           id_vars=general_columns,
                           value_vars=cond_occ_columns)

    condition_df = condition_df.loc[condition_df["value"] == ISARICYesNo.yes]
    condition_df["condition_start_date"] = condition_df.apply(
        lambda x: x["cestdat"] if pd.notnull(x["cestdat"]) else x["dsstdat"], axis="columns")
    condition_df["condition_type_concept_id"] = visit_concept_type.concept_id
    condition_df["condition_concept_id"] = condition_df["variable"].apply(
        lambda x: getattr(OMOPConditions, x))
    # todo add smoking status and other conditions
    condition_df = condition_df.loc[pd.notnull(condition_df["condition_concept_id"])]
    condition_df = prepare_autoincrement_index(condition_df,
                                               "condition_occurrence",
                                               "condition_occurrence_id",
                                               postgres)
    condition_df = condition_df.reindex(columns=OMOP_CONDITION_HEADER)
    postgres.df_to_postgres(table="condition_occurrence", df=condition_df)
