import json
import logging
import pandas as pd
from pandas import DataFrame

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_df_from_json(response_data: dict) -> DataFrame:
    """
    Convert a nested dictionary or JSON-like object into a flat pandas DataFrame.

    Args:
        response_data (dict): A single JSON/dict object to normalize.

    Returns:
        pd.DataFrame: A flattened DataFrame representing the JSON structure.

    Raises:
        ValueError: If response_data is not a dictionary.
    """
    if not isinstance(response_data, dict):
        logger.error("Input data is not a dictionary.")
        raise ValueError("Input must be a dictionary.")

    try:
        df = pd.json_normalize(response_data)
        logger.debug("Successfully normalized JSON data into DataFrame.")
        return df
    except Exception as e:
        logger.exception("Failed to normalize JSON to DataFrame.")
        raise


def serialize_dict_columns(df: DataFrame) -> DataFrame:
    """
    Serialize any columns containing Python dictionaries into JSON strings.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with dict-type columns converted to JSON strings.

    Notes:
        This is helpful before inserting into SQL databases which don't support raw dicts.
    """
    try:
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                logger.debug(f"Serializing column '{col}' with dict values.")
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
        return df
    except Exception as e:
        logger.exception("Failed to serialize dictionary columns.")
        raise


def clean_for_sql(df: DataFrame) -> DataFrame:
    """
    Convert all dictionary values in the DataFrame to JSON strings.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame safe for SQL insertions.

    Notes:
        Uses applymap to clean every cell in the DataFrame.
    """
    try:
        cleaned_df = df.applymap(lambda cell: json.dumps(cell) if isinstance(cell, dict) else cell)
        logger.debug("Successfully cleaned DataFrame for SQL insertion.")
        return cleaned_df
    except Exception as e:
        logger.exception("Failed to clean DataFrame for SQL.")
        raise