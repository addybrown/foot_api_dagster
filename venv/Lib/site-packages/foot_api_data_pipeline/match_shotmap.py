import pandas as pd
import numpy as np
import datetime
import os
import time
from dotenv import load_dotenv

load_dotenv()
all_sports_api_key = os.getenv("all_sports_api_key")

from services.sql_services import (
    read_sql,
    write_sql,
    check_table_exists,
    create_session,
    bulk_upsert_write_sql,
)

from services.pandas_services import adjust_col_name_format
from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import (
    MATCH_SHOTMAP_DATAFRAME,
    PLAYER_TABLE_VARIABLES,
    RELEVANT_LEAGUES,
)
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_match_shotmap_table(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=5,
    api_key=None,
    date_range=None,
    start_date=None,
    end_date=None,
    league_list=None,
    **kwargs,
):
    """

    This function updates the shot_details table, for all data that does not currently exist for the match_ids
    in the following three days, if match_id data already exists, it will not update those games.

    Args:
        db_table (str, optional): the database name. Defaults to None.
        db_schema (str, optional): the database schema name. Defaults to None.
        session (sql.alchemy.engine, optional): the database session connection. Defaults to None.
        wait_time (int, optional): the wait_time is defined as the time between api calls. Defaults to 5.
        api_key (str, optional): api_key value. Defaults to None.
        date_range (int, optional): _description_. Defaults to 3.
    """
    if not session:
        session = create_session()

    if not db_table:
        db_table = "match_shotmap"

    if not db_schema:
        db_schema = "football_data"

    if not api_key:
        api_key = all_sports_api_key

    if start_date:
        if not end_date:
            raise Exception("You need end_date with start_date parameter")

    if not league_list:
        league_list = RELEVANT_LEAGUES

    # Creates Default table in case of deletion
    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        shot_df = get_match_shotmap_df(match_id="10408546", api_key=api_key)
        write_sql(
            shot_df,
            db_table=db_table,
            db_schema=db_schema,
            session=session,
            terminate_connection=True,
        )

    country_list = PLAYER_TABLE_VARIABLES["player_table_country_names"]

    schedule_df = get_schedule(
        country_list=country_list,
        date_range=date_range,
        start_date=start_date,
        end_date=end_date,
        league_list=league_list,
        session=session,
    )

    match_ids_schedule = schedule_df["match_id"].unique().tolist()

    shot_sql_string = f"select match_id,shot_id FROM {db_schema}.{db_table}"
    shot_df = read_sql(shot_sql_string, session=session)

    current_shot_match_ids = shot_df["match_id"].unique().tolist()
    current_shot_ids = shot_df["shot_id"].unique().tolist()

    match_id_list_for_shot = list(
        set(match_ids_schedule).difference(set(current_shot_match_ids))
    )

    for match_id in match_id_list_for_shot:
        shot_df = get_match_shotmap_df(match_id=match_id, api_key=api_key)

        if not shot_df.empty:
            shot_df = shot_df.query(f"shot_id!={current_shot_ids}")
            bulk_upsert_write_sql(
                shot_df, dbtable=db_table, dbschema=db_schema, session=session
            )
        time.sleep(wait_time)


def get_match_shotmap_df(match_id, shot_response_json=None, api_key=None, **kwargs):
    """

    generates a table of single match shot values for a given match_id

    Args:
        match_id (str): the match_id input for the all_sports_api
        shot_response_json (json): the all_sports_api json file value
        api_key (str): the all_sports_api key

    Returns:
        _type_: dataframe of single shot dataframe
    """
    try:
        if not shot_response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            shot_response_json = api_client.get_match_response_json(
                match_id=match_id, stat_type="shotmap"
            )

        shot_map_df = pd.json_normalize(shot_response_json["shotmap"])
        shot_map_df.rename(columns={"id": "shot_id"}, inplace=True)

        shot_map_df = adjust_col_name_format(shot_map_df)
        shot_map_df["match_id"] = match_id

        main_cols = MATCH_SHOTMAP_DATAFRAME["main_cols"]

        for col in main_cols:
            if col not in shot_map_df.columns:
                shot_map_df[col] = None

        shot_map_df = shot_map_df[main_cols]

        shot_map_df["created_on"] = datetime.datetime.utcnow()

        return shot_map_df

    except Exception as exc:
        print(
            f"match_id:{match_id}:{shot_response_json}{exc}: get_all_sports_api_single_match_shot_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()
