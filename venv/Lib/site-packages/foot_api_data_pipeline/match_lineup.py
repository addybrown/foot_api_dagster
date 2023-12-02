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
    execute_sql,
    check_table_exists,
    create_session,
    bulk_upsert_write_sql,
)

from services.pandas_services import adjust_col_name_format
from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import (
    MATCH_LINEUP_DATAFRAME,
    PLAYER_STATISTICS,
    PLAYER_TABLE_VARIABLES,
    RELEVANT_LEAGUES,
)
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_match_lineup_and_player_statistics(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=3,
    api_key=None,
    date_range=3,
    league_list=None,
    start_date=None,
    end_date=None,
    **kwargs,
):
    """

    This function updates all the player lineups, formation and statistic tables that exists in the database
    for the previous three days, if data already exists it will not be updated.

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
        db_table = "lineups"

    if not db_schema:
        db_schema = "football_data"

    if not api_key:
        api_key = all_sports_api_key

    if not league_list:
        league_list = RELEVANT_LEAGUES

    if start_date:
        if not end_date:
            raise Exception("You need end_date with start_date parameter")

    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        sql_table_creation = f"""
        CREATE TABLE {db_schema}.{db_table} (
            
                    match_id text,
                    home_team_id text,
                    home_team_name text,
                    home_formation text,
                    away_team_id text,
                    away_team_name text,
                    away_formation text
                );
                        
        """
        execute_sql(sql_table_creation, session=session)

    country_list = PLAYER_TABLE_VARIABLES["player_table_country_names"]

    schedule_df = get_schedule(
        country_list=country_list,
        date_range=date_range,
        start_date=start_date,
        end_date=end_date,
        league_list=league_list,
        session=session,
    )
    schedule_df["match_id"] = schedule_df["match_id"].astype(str)
    schedule_match_ids = schedule_df["match_id"].unique().tolist()

    lineup_sql_string = f"SELECT * FROM {db_schema}.{db_table}"
    lineup_database_df = read_sql(lineup_sql_string, session=session)
    lineup_database_df["match_id"] = lineup_database_df["match_id"].astype(str)
    lineup_database_match_ids = lineup_database_df["match_id"].unique().tolist()

    remaining_match_ids = list(
        set(schedule_match_ids).difference(set(lineup_database_match_ids))
    )
    for match_id in remaining_match_ids:
        lineup_df = get_match_lineup_df(match_id=match_id, api_key=api_key)
        # lineup_df = lineup_df.query(f"match_id !={lineup_database_match_ids}")
        if not lineup_df.empty:
            new_lineup_df = pd.merge(
                lineup_df,
                schedule_df[
                    [
                        "match_id",
                        "home_team_id",
                        "home_team_name",
                        "away_team_id",
                        "away_team_name",
                    ]
                ],
                on="match_id",
            )

            lineup_cols = [
                "match_id",
                "home_team_id",
                "home_team_name",
                "home_formation",
                "away_team_id",
                "away_team_name",
                "away_formation",
            ]

            final_lineup_df = new_lineup_df[lineup_cols]
            final_lineup_df["created_on"] = datetime.datetime.utcnow()

            bulk_upsert_write_sql(
                final_lineup_df, dbtable=db_table, dbschema=db_schema, session=session
            )

            statistics_dicts = get_player_statistics_dicts(
                new_lineup_df, session=session
            )
            write_player_statistics_tables(statistics_dicts, session=session)

            time.sleep(wait_time)


def get_match_lineup_df(match_id, response_json=None, api_key=None, **kwargs):
    """

        generates the match_lineup dataframe

    Args:
        match_id (str): _description_
        match_lineup_response_json (dict, optional): the match_lineup response json. Defaults to None.
        api_key (str, optional): the api key. Defaults to None.

    Returns:
        lineup_df: the match lineup dataframe for the match_id
    """
    try:
        if not response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            response_json = api_client.get_match_response_json(
                match_id=match_id, stat_type="lineups"
            )

        lineup_df = pd.json_normalize(response_json)
        lineup_df = adjust_col_name_format(lineup_df)

        lineup_df["match_id"] = match_id

        print(lineup_df.head())

        lineup_df = lineup_df[MATCH_LINEUP_DATAFRAME["main_cols"]]

        return lineup_df

    except Exception as exc:
        print(
            f"{exc}:get_all_sports_api_match_lineup_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()


def get_player_statistics_dicts(lineup_df, session=None):
    """

    This function unpacks the player_statistics from the raw lineup_df dataframe
    and organizing it into the following tables:
    "player_rating", "player_passing", "player_shooting", "player_defense", "player_mistake", "player_misc"

    Args:
        lineup_df (pd.DataFrame): the lineup_df dataframe which is pulled from api source
        session (sqlalchemy.enging, optional): the sqlalchemy engine value. Defaults to None.

    Returns:
        dict: this returns a dictionary of statistics values with the following format:
            statistics_dicts["table_name"] = table_name_dataframe

    Raises:
        Error that function did not work
    """
    if not session:
        session = create_session()

    shared_cols = PLAYER_STATISTICS["shared_cols"]

    tables = [
        "player_rating",
        "player_passing",
        "player_shooting",
        "player_defense",
        "player_mistake",
        "player_misc",
    ]

    lineup_df = adjust_col_name_format(lineup_df)

    statistics_dicts = {}
    rename_cols = PLAYER_STATISTICS["rename_cols"]

    home_stats_df = adjust_col_name_format(
        pd.json_normalize(lineup_df["home_players"].iloc[0]).rename(columns=rename_cols)
    )
    away_stats_df = adjust_col_name_format(
        pd.json_normalize(lineup_df["away_players"].iloc[0]).rename(columns=rename_cols)
    )

    home_stats_df["home_away"] = "home"
    away_stats_df["home_away"] = "away"

    match_id = lineup_df["match_id"].iloc[0]

    home_stats_df["match_id"] = match_id
    away_stats_df["match_id"] = match_id

    home_stats_df["team_id"] = lineup_df["home_team_id"].iloc[0]
    home_stats_df["team_name"] = lineup_df["home_team_name"].iloc[0]

    away_stats_df["team_id"] = lineup_df["away_team_id"].iloc[0]
    away_stats_df["team_name"] = lineup_df["away_team_name"].iloc[0]

    for table in tables:
        table_cols = shared_cols + PLAYER_STATISTICS[table]["main_cols"]
        for col in table_cols:
            if col not in home_stats_df.columns:
                home_stats_df[col] = None

            if col not in away_stats_df.columns:
                away_stats_df[col] = None

        statistics_dicts[table] = pd.concat(
            [home_stats_df[table_cols], away_stats_df[table_cols]], axis=0
        )

    return statistics_dicts


def write_player_statistics_tables(statistics_dict, db_schema=None, session=None):
    """

    This function writes the dataframes inside statistics dict to the database table

    Args:
        statistics_dict (dict): a dictionary of statistics_dict
        db_schema (str, optional): the database schema name. Defaults to None.
        session (sqlalchemy.engine, optional): the connection engine for sqlalchemy. Defaults to None.

    Returns:
        None

    Raises:
        Error that function did not work

    """
    if not session:
        session = create_session()

    if not db_schema:
        db_schema = "football_data"

    tables = [
        "player_rating",
        "player_passing",
        "player_shooting",
        "player_defense",
        "player_mistake",
        "player_misc",
    ]

    for table in tables:
        sql_string = f"select distinct concat(match_id,'_',player_id) as match_id_player_id from {db_schema}.{table}"
        # sql_string = f"select * from {db_schema}.{table} Limit 1"
        existing_vals_df = read_sql(sql_string, session=session)

        existing_columns = existing_vals_df.columns.tolist()

        existing_vals = existing_vals_df["match_id_player_id"].unique().tolist()

        statistics_dict[table]["match_id_player_id"] = (
            statistics_dict[table]["match_id"].astype(str)
            + "_"
            + statistics_dict[table]["player_id"].astype(str)
        )

        statistics_dict[table] = statistics_dict[table].query(
            f"match_id_player_id != {existing_vals}"
        )
        statistics_dict[table] = statistics_dict[table].drop(
            columns=["match_id_player_id"], axis=1
        )
        statistics_dict[table]["created_on"] = datetime.datetime.utcnow()

        bulk_upsert_write_sql(
            statistics_dict[table], dbtable=table, dbschema=db_schema, session=session
        )
