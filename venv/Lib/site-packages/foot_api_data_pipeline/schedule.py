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
from foot_api_data_pipeline.variables import SCHEDULE_VARIABLES


def update_schedule_table(
    start_date=None,
    end_date=None,
    db_table=None,
    db_schema=None,
    session=None,
    wait_time=5,
    api_key=None,
):
    """

    This function updates the football_data schedule, using the FootApi, data is collected in json format,
    transformed and sent to the database for the date_range between the start_date and end_date, if match_ids already exist
    they were not be re-updated to the schedule table. By default this functions runs for 3 days.

    Args:
        start_date (str, optional): the start date to update the schedule table. Defaults to None.
        end_date (str, optional): the end date to update the schedule table. Defaults to None.
        db_table (str, optional): the database name. Defaults to None.
        db_schema (str, optional): the database schema name. Defaults to None.
        session (session, optional): the database session connection. Defaults to None.
        wait_time (int, optional): the wait_time is defined as the time between api calls. Defaults to 5.
        api_key (str, optional): api_key value. Defaults to None.

    """
    if not db_table:
        db_table = "schedule"

    if not db_schema:
        db_schema = "football_data"

    if not session:
        session = create_session()

    if not start_date:
        start_date = datetime.datetime.today() - datetime.timedelta(3)
    else:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

    if not end_date:
        end_date = datetime.datetime.today()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    if not api_key:
        api_key = all_sports_api_key

    date_list = [
        start_date + datetime.timedelta(days=x)
        for x in range((end_date - start_date).days)
    ]

    # If empty dataframe initialize
    if not check_table_exists(db_table=db_table, db_schema=db_schema, session=session):
        schedule_df = get_schedule_df("1", "1", "2023")
        write_sql(
            schedule_df,
            db_table=db_table,
            db_schema=db_schema,
            session=session,
            terminate_connection=True,
        )

    for date in date_list:
        day = str(date.day)
        month = str(date.month)
        year = str(date.year)

        schedule_df = get_schedule_df(day, month, year)
        schedule_df["match_id"] = schedule_df["match_id"].astype(str)

        # bulk_upsert_write_sql(df=schedule_df, dbtable=db_table, dbschema=db_schema, session=session)

        existing_schedule_df = read_sql(
            f"SELECT * from {db_schema}.{db_table}", session=session
        )

        existing_columns = existing_schedule_df.columns.tolist()

        if not schedule_df.empty:
            match_id_difference = list(
                set(schedule_df["match_id"].unique().tolist()).difference(
                    set(existing_schedule_df["match_id"].unique().tolist())
                )
            )

            schedule_df = schedule_df.query(f"match_id == {match_id_difference}")

            if not schedule_df.empty:
                schedule_df["created_on"] = datetime.datetime.utcnow()
                bulk_upsert_write_sql(
                    schedule_df[existing_columns],
                    dbtable=db_table,
                    dbschema=db_schema,
                    session=session,
                    # terminate_connection=True,
                )
                time.sleep(wait_time)


def get_schedule_df(
    day, month, year, match_schedule_response_json=None, api_key=None, **kwargs
):
    """

    generates match schedule dataframe

    Args:
        day (str): day of match_schedule
        month (str): _description_
        year (str): _description_
        match_schedule_response_json (jason): match schedule response json
        api_key (str): the all_sports_api_key

    Returns:
        _type_: dataframe of match schedule
    """
    try:
        if not match_schedule_response_json:
            if not api_key:
                api_key = all_sports_api_key

            api_client = FootApiHarvester(api_key=api_key)
            match_schedule_response_json = api_client.get_schedule_response_json(
                day=day, month=month, year=year
            )

        rename_cols = SCHEDULE_VARIABLES["rename_cols"]
        main_cols = SCHEDULE_VARIABLES["main_cols"]

        match_schedule_df = pd.json_normalize(match_schedule_response_json["events"])
        match_schedule_df = adjust_col_name_format(match_schedule_df)

        match_schedule_df.rename(columns=rename_cols, inplace=True)
        match_schedule_df = match_schedule_df[main_cols]

        match_schedule_df["match_start_time"] = match_schedule_df[
            "match_start_time"
        ].apply(
            lambda x: datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
        )
        match_schedule_df["created_on"] = datetime.datetime.utcnow()
        # match_schedule_df["league_id"] = match_schedule_df["league_id"].astype(int)

        return match_schedule_df

    except Exception as exc:
        print(
            f"{exc}: get_all_sports_api_match_schedule_dataframe was unable to process, check api_key or source data"
        )
        return pd.DataFrame()
