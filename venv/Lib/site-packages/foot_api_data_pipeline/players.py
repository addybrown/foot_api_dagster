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
    execute_sql,
    bulk_upsert_sql,
)

from services.pandas_services import adjust_col_name_format
from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import (
    PLAYER_TABLE_VARIABLES,
    PLAYER_TEAM_DATAFRAME,
    DAILY_UPLOAD_FUNCTIONS_LOG_PATHS,
)
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_player_table(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=5,
    api_key=None,
    date_range=3,
    start_date=None,
    end_date=None,
    **kwargs,
):
    """

    This function updates the player table in the database, this is done based on the previous three days
    by default.

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
        db_table = "players"

    if not db_schema:
        db_schema = "football_data"

    if not api_key:
        api_key = all_sports_api_key

    if start_date:
        if not end_date:
            raise Exception("You need end_date with start_date parameter")

    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        player_df = get_team_player_df(team_id="3", api_key=api_key)
        write_sql(
            player_df,
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
        session=session,
    )

    schedule_team_ids = (
        schedule_df["home_team_id"].unique().tolist()
        + schedule_df["away_team_id"].unique().tolist()
    )

    player_sql_string = f"SELECT DISTINCT team_id from {db_schema}.{db_table}"
    player_df = read_sql(player_sql_string, session=session)

    player_unique_team_ids = player_df["team_id"].unique().tolist()

    difference_ids_list = set(schedule_team_ids).difference(set(player_unique_team_ids))

    on_conflict_columns = ["player_id"]

    set_columns = [
        "player_name",
        "player_position",
        "date_of_birth",
        "jersey_number",
        "preferred_foot",
        "player_height",
        "market_value",
        "market_value_currency",
        "team_id",
        "team_name",
        "team_national",
        "league_id",
        "league_name",
        "team_country_name",
        "player_nationality",
        "created_on",
    ]

    for team_id in difference_ids_list:
        player_df = get_team_player_df(
            team_id=str(team_id), api_key=api_key, log_failure=False
        )
        if not player_df.empty:
            player_df["player_id"] = player_df["player_id"].astype(str)
            player_df["player_name"] = player_df["player_name"].apply(
                lambda x: x.replace("'", "''")
            )
            bulk_upsert_sql(
                df=player_df,
                db_schema=db_schema,
                db_table=db_table,
                on_conflict_columns=on_conflict_columns,
                set_columns=set_columns,
            )
            # write_sql(df=player_df, db_table=db_table, db_schema=db_schema, session=session, terminate_connection=True)
        time.sleep(wait_time)

    update_none_values_in_player_table(db_table, db_schema, session)


def get_team_player_df(
    team_id, team_player_response_json=None, api_key=None, log_failure=True
):
    """

    generates all_sports_api player table for each team

    Args:
        team_id (str): the team_id of the team
        team_player_response_json (json): the json response value for team_id
        api_key (str): the all_sports_api api_key required

    Returns:
        _type_: dataframe of team_player_schedule
    """

    try:
        if not team_player_response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            team_player_response_json = api_client.get_team_response_json(
                team_id, "players"
            )

        rename_cols = PLAYER_TEAM_DATAFRAME["rename_cols"]
        main_cols = PLAYER_TEAM_DATAFRAME["main_cols"]

        team_player_df = pd.json_normalize(team_player_response_json["players"])

        team_player_df = adjust_col_name_format(team_player_df)
        team_player_df.rename(columns=rename_cols, inplace=True)

        for col in main_cols:
            if col not in list(team_player_df.columns):
                team_player_df[col] = None

        team_player_df = team_player_df[main_cols]

        team_player_df = team_player_df.query("player_id == player_id")
        final_team_player_df = team_player_df
        final_team_player_df["date_of_birth"] = final_team_player_df[
            "date_of_birth"
        ].apply(
            lambda x: datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d")
            if str(x) != "nan"
            else x
        )
        final_team_player_df = final_team_player_df.fillna("None")
        final_team_player_df["created_on"] = datetime.datetime.utcnow()

        return final_team_player_df

    except Exception as exc:
        print(
            f"{exc} : team_id - {team_id}: get_all_sports_api_team_player_dataframe was unable to process, check api_key or source data, also possible rate limit error"
        )

        if log_failure:
            log_path = os.path.join(
                DAILY_UPLOAD_FUNCTIONS_LOG_PATHS["player_table"],
                "player_table_logs.txt",
            )
            with open(log_path, "a") as file:
                file.write(f"{str(team_id)}\n")

        return pd.DataFrame()


def update_none_values_in_player_table(db_table, db_schema, session=None):
    """
    updates the 'none' values in the player table

    Args:
        team_player_df (pd.DataFrame): the team player dataframe
        session (sqlalchemy.engine): the session of the database

    Returns:
        None

    Raises:
        Error that function did not work
    """
    if not session:
        session = create_session()

    team_player_df = read_sql(
        f"SELECT * FROM {db_schema}.{db_table} LIMIT 3", session=session
    )

    for column in team_player_df.columns:
        if column != "created_on":
            sql_string = f"""
            UPDATE {db_schema}.{db_table} 
            SET {column} = null
            WHERE {column} = 'None'
            """
            execute_sql(sql_string, session=session)
