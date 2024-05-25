import logging
import pandas as pd
import re
from typing import List

from core.db_connector import PostgresController

log = logging.getLogger(__name__)


def increment_last_id(table: str, column: str, postgres: PostgresController):
    query = f"SELECT MAX({column}) FROM {postgres.cdm_schema}.{table};"
    result = postgres.postgres_fetch(query)["max"].values.tolist()[0]
    if result is None:
        return 1
    return result + 1


def prepare_autoincrement_index(df: pd.DataFrame,
                                table: str,
                                index_name: str,
                                postgres: PostgresController) -> pd.DataFrame:
    increment_by_index = increment_last_id(table, index_name, postgres)
    df.reset_index(drop=True, inplace=True)
    df.index += increment_by_index
    df.index.name = index_name
    df.reset_index(drop=False, inplace=True)
    return df


def validate_concept_domain(postgres: PostgresController, ids_in_question: List[str], domain_id: str) -> None:
    """
    Validates if all concept ids are in the database and of expected domain
    :param postgres: postgres client
    :param ids_in_question: list of integer strings ids
    :param domain_id: str, a name of a domain
    :return: None
    """
    query = f"select * from vocabularies.concept where concept_id in ({', '.join(ids_in_question)});"

    result = postgres.postgres_fetch(query)

    missing_concepts = [x for x in ids_in_question if x not in result["concept_id"].values]
    if missing_concepts:
        log.warning(f"Following concept ids are missing from the database: {', '.join(missing_concepts)}")

    unexpected_domain = result.loc[result["domain_id"] != domain_id]
    if not unexpected_domain.empty:
        records = unexpected_domain.to_dict("records")
        for item in records:
            log.warning(
                f"Concept is not of {domain_id} domain: concept_name {item['concept_name']}, "
                f"domain: {item['domain_id']}, concept_id: {item['concept_id']}")


def merge_columns_with_postfixes(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    pattern = re.escape(column_name) + r"___[1-9][0-9]?"
    columns_with_postfix = [c for c in df.columns if re.match(pattern, c)]
    if column_name in df.columns and pd.isnull(df[column_name]).all():
        for column in columns_with_postfix:
            postfix = column.split("_")[-1]
            df.loc[df[column].isin([1, 1.0, "1"]), column_name] = postfix
    df.drop(columns=columns_with_postfix, inplace=True)
    return df
