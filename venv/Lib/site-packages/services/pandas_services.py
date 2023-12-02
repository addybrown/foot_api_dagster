import pandas as pd


def convert_datetime_timestamp_cols_to_string(
    df: pd.DataFrame, target_columns: list = None
):
    """
    Converts rows where the object data_type is datetime, timestamp
    or pandas/numpy variations of that to string, to be used mainly for
    upserting to sql tables.

    inputs:

    * df: the dataframe which requires converting
    * dbschema: the target schema of the wanted table.
    * dbtable: the target table
    * dbsession: a db connection if exists, if not created in default
    * terminate_connection: terminates db connection after being used

    returns: pd.DataFrame where df columns are reordered as a dbtable columns ordering

    raises: IndexError for column lengths not matching between dataframe and target table.
    """

    target_columns = target_columns if target_columns else list(df.columns)
    for column in target_columns:
        try:
            string_val = str(type(df[column].iloc[0])).lower()
            if "datetime" in string_val or "timestamp" in string_val:
                df[column] = df[column].astype(str)

        except Exception as exc:
            error_message = "continuing process"
            df[column] = df[column].astype(str, errors="ignore")

    df.replace({r"\bNone\b": None}, inplace=True, regex=True)
    df.replace({r"\bnan\b": None}, inplace=True, regex=True)

    return df


def adjust_col_name_format(df):
    new_cols = []
    for col in df.columns:
        if "." in col:
            val = col.replace(".", "_")
        else:
            val = col

        new_cols.append(val)

    df.columns = new_cols

    return df


def append_to_dict_list(append_key: str, append_value: str, list_dict: list):
    """
    This function appends a key value pair to a list of dictionaries, specifically when
    you have a column of a dictionary list

    :param append_key: the append key value i.e. (player_id)
    :param append_value: the value for the append key i.e. (value = "10", appended_dict_value = {"player_id":"10"})
    :param list_dict: the list of dictionaries

    :return:
    dataframe of different values
    """
    result = [{**item, append_key: append_value} for item in list_dict]
    return result
