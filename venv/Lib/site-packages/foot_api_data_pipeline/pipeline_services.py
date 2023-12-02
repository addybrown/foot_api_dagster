import datetime
from dotenv import load_dotenv

load_dotenv()
from services.sql_services import (
    read_sql,
    create_session,
)


def get_schedule(
    db_table=None,
    db_schema=None,
    country_list=None,
    date_range=None,
    start_date=None,
    end_date=None,
    league_list=None,
    order_dates=True,
    match_id_list=None,
    session=None,
):
    """

    This function generates the schedule dataframe from the database connection session
    based on the input parameters provided.

    Args:
        db_table (str, optional): the database table name. Defaults to None.
        db_schema (str, optional): the database schema name. Defaults to None.
        country_list (list, optional): the list of countries for schedule. Defaults to None.
        date_range (int, optional): the date range to look at. Defaults to None.
        start_date (str, optional): the start date for schedule to query. Defaults to None.
        end_date (str, optional): the max date for schedule to query. Defaults to None.
        order_dates (bool, optional): orders date if true, random if false. Defaults to True.
        session (sqlalchemy.engine, optional): the database session . Defaults to None.

    Raises:
        Exception: _description_

    Returns:
        pd.DataFrame: dataframe of schedule
    """

    try:
        if not db_table:
            db_table = "schedule"

        if not db_schema:
            db_schema = "football_data"

        if not session:
            session = create_session()

        country_list_str = ""
        if country_list:
            country_list_str = f"AND country_name in {country_list}"

        league_list_str = ""
        if league_list:
            league_list_str = f"AND league_name in {league_list}"

        date_range_str = ""
        if date_range and not end_date:
            end_date = datetime.datetime.today()
            start_date = datetime.datetime.today() - datetime.timedelta(date_range)

            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

            date_range_str = (
                f"AND DATE(match_start_time) BETWEEN '{start_date}' AND '{end_date}' "
            )

        if start_date:
            if end_date:
                date_range_str = f"AND DATE(match_start_time) BETWEEN '{start_date}' AND '{end_date}' "
            else:
                error_string = "You need a end_date if start_date is not None"
                raise Exception(error_string)

        match_ids_list_string = ""
        if match_id_list:
            match_ids_list_string = f"AND match_id in {match_id_list}"

        order_dates_str = {}
        if order_dates:
            order_dates_str = "ORDER BY DATE(match_start_time)"

        schedule_df_sql_string = f"""
        SELECT * FROM {db_schema}.{db_table} 
            WHERE tournament_name is not null
                {country_list_str}
                {date_range_str}
                {league_list_str}
                {order_dates_str}
                {match_ids_list_string}
        """

        schedule_df = read_sql(schedule_df_sql_string, session=session)

        return schedule_df

    except Exception as exc:
        raise Exception(
            f"function: ..all_sports_api_data_processing.schedule_table_utils.utils.get_schedule failed: {exc}"
        )
