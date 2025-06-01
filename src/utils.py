import json
import pandas as pd


def create_df_from_json(response_data: dict) -> pd.DataFrame:
    """
    Convert nested JSON/dict data into a flat pandas DataFrame.

    Args:
        response_data (dict): Raw JSON response or dictionary to normalize.

    Returns:
        pd.DataFrame: Normalized DataFrame.
    """
    return pd.json_normalize(response_data)


def serialize_dict_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Serialize any dictionary-type columns in a DataFrame to JSON strings.

    Args:
        df (pd.DataFrame): DataFrame to process.

    Returns:
        pd.DataFrame: DataFrame with dict columns serialized.

    Notes:
        This is useful before inserting data into SQL databases,
        which do not natively support Python dicts.
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return df


def clean_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recursively convert all dict cells in a DataFrame to JSON strings.

    Args:
        df (pd.DataFrame): DataFrame to clean.

    Returns:
        pd.DataFrame: Cleaned DataFrame with all dict cells serialized.
    """
    def convert_cell(cell):
        return json.dumps(cell) if isinstance(cell, dict) else cell

    return df.applymap(convert_cell)
