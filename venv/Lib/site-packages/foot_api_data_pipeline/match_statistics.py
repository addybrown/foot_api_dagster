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

from foot_api_harvesting.utils import FootApiHarvester
from foot_api_data_pipeline.variables import PLAYER_TABLE_VARIABLES, RELEVANT_LEAGUES
from foot_api_data_pipeline.pipeline_services import get_schedule


def update_match_statistics_table(
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=5,
    api_key=None,
    date_range=None,
    start_date=None,
    end_date=None,
    country_list=None,
    league_list=None,
    **kwargs,
):
    """

    This function updates the match_statistics table, for all data that does not currently exist for the match_ids
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
        db_table = "match_statistics"

    if not db_schema:
        db_schema = "football_data"

    if not api_key:
        api_key = all_sports_api_key

    if start_date:
        if not end_date:
            raise Exception("You need end_date with start_date parameter")

    if not country_list:
        country_list = PLAYER_TABLE_VARIABLES["player_table_country_names"]

    if not league_list:
        league_list = RELEVANT_LEAGUES

    # Creates Default table in case of deletion
    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        match_statistics_df = get_match_statistics_df(
            match_id="9711852", api_key=api_key
        )

        write_sql(
            match_statistics_df,
            db_table=db_table,
            db_schema=db_schema,
            session=session,
            terminate_connection=True,
        )

    schedule_df = get_schedule(
        db_schema=db_schema,
        country_list=country_list,
        date_range=date_range,
        start_date=start_date,
        end_date=end_date,
        league_list=league_list,
        session=session,
    )

    match_ids_schedule = schedule_df["match_id"].unique().tolist()

    match_statistics_sql_string = (
        f"select distinct match_id FROM {db_schema}.{db_table}"
    )
    current_match_statistics_df = read_sql(match_statistics_sql_string, session=session)

    current_match_incident_match_ids = (
        current_match_statistics_df["match_id"].unique().tolist()
    )

    match_id_list_remaining = list(
        set(match_ids_schedule).difference(set(current_match_incident_match_ids))
    )

    for match_id in match_id_list_remaining:
        match_statistics_df = get_match_statistics_df(
            match_id=match_id, api_key=api_key
        )

        if not match_statistics_df.empty:
            bulk_upsert_write_sql(
                match_statistics_df,
                dbtable=db_table,
                dbschema=db_schema,
                session=session,
            )

        time.sleep(wait_time)


def get_match_statistics_df(match_id, response_json=None, api_key=None, **kwargs):
    try:
        if not response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            response_json = api_client.get_match_response_json(
                match_id=match_id, stat_type="statistics"
            )

        all_dfs = []
        for value in response_json["statistics"]:
            df = pd.json_normalize(value)
            period_value = df["period"].iloc[0]
            group_value = df["groups"].iloc[0]

            group_df = pd.json_normalize(group_value)

            for value2 in group_df.groupName:
                group_name = value2
                final_df = pd.json_normalize(
                    group_df.query(f"groupName == '{value2}' ")["statisticsItems"].iloc[
                        0
                    ]
                )
                final_df["period"] = period_value
                final_df["stat_type"] = group_name

                all_dfs.append(final_df)

        match_stats_df = pd.concat(all_dfs, axis=0)
        match_stats_df["match_id"] = match_id

        columns = ["match_id", "name", "stat_type", "home", "away", "period"]

        match_stats_df = match_stats_df[columns]

        match_stats_df = match_stats_df.reset_index(drop=True)

        return match_stats_df

    except Exception as exc:
        print(
            f"{exc}:get_all_sports_api_match_statistics_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()
