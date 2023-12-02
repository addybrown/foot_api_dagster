import sqlalchemy
import os
import pandas as pd
import logging

from services.pandas_services import convert_datetime_timestamp_cols_to_string

database_user_name = os.environ.get("mysql_user_name")
database_user_name = "root"
database_pw = os.environ.get("mysql_pw")
host_name = os.environ.get("mysql_host_name")
port_value = os.environ.get("mysql_port_value")
database_name_str = os.environ.get("mysql_database_name")

connector = "mysql+mysqlconnector"


def format_sql_string(sql_string, format_brackets=True):
    """
    SQL strings must respect the conditions for a SQL query
    inputs:
    * sql_string (str): the sql string requiring a format

    returns:
    * sql_string formatted for sql query
    """
    # format change: ['test'] -> ('test')
    if format_brackets:
        sql_string = sql_string.replace("[", "(")
        sql_string = sql_string.replace("]", ")")

    return sql_string


def create_session(
    user_name=None, password=None, port=None, host=None, database_name=None
):
    if not user_name:
        user_name = database_user_name

    if not password:
        password = database_pw

    if not host:
        host = host_name

    if not port:
        port = port_value

    if not database_name:
        database_name = database_name_str

    url = f"{connector}://{user_name}:{password}@{host}:{port}/{database_name}"
    engine = sqlalchemy.create_engine(
        url, echo=True, pool_size=20, max_overflow=10, pool_pre_ping=True
    )

    return engine


def read_sql(sql_string, session=None, terminate_connection=False, **kwargs):
    if not session:
        session = create_session()

    connection = session.connect()

    sql_string = format_sql_string(sql_string)

    df = pd.read_sql(sql=sql_string, con=connection, **kwargs)

    if terminate_connection:
        connection.close()

    return df


def write_sql(
    df, db_table, db_schema, session=None, terminate_connection=False, **kwargs
):
    if not session:
        session = create_session()

    connection = session.connect()

    df.to_sql(
        name=db_table,
        schema=db_schema,
        con=connection,
        if_exists="append",
        index=False,
        chunksize=10000,
    )

    if terminate_connection:
        connection.close()


def execute_sql(sql_string, session=None, terminate_connection=True, **kwargs):
    if not session:
        session = create_session()

    connection = session.connect()

    sql_string = format_sql_string(sql_string)

    statement = sqlalchemy.text(sql_string)

    connection.execute(statement)

    if terminate_connection:
        connection.close()


def check_table_exists(db_table, db_schema, session=None):
    if not session:
        session = create_session()

    value = sqlalchemy.inspect(session).has_table(db_table, schema=db_schema)

    return value


def delete_table_duplicates(primary_key, db_table, db_schema, session=None):
    if not session:
        session = create_session()

    delete_sql_string = f"""
                    DELETE FROM {db_schema}.{db_table}
                WHERE
                    {primary_key} IN (
                    SELECT
                        {primary_key}
                    FROM (
                        SELECT
                            {primary_key},
                            ROW_NUMBER() OVER (
                                PARTITION BY {primary_key}
                                ORDER BY {primary_key}) AS row_num
                        FROM
                            {db_schema}.{db_table}

                    ) t
                    WHERE row_num > 1
                );               
    """

    execute_sql(delete_sql_string, session=session)


def delete_duplicates(db_table, db_schema, session=None, create_unique_index=False):
    if not session:
        session = create_session()

    string_value = f"""
        CREATE TABLE {db_schema}.{db_table}_temp (LIKE {db_schema}.{db_table});
    """

    execute_sql(string_value, session=session)

    string_value = f"""
    INSERT INTO {db_schema}.{db_table}_temp(
        SELECT DISTINCT *
        FROM {db_schema}.{db_table});
    """
    execute_sql(string_value, session=session)

    string_value = f"""
            DROP TABLE {db_schema}.{db_table};
        
    """
    execute_sql(string_value, session=session)

    string_value = f"""
        RENAME TABLE {db_schema}.{db_table}_temp to {db_schema}.{db_table};
    """
    execute_sql(string_value, session=session)

    if create_unique_index:
        string_value = f"""
                CREATE UNIQUE INDEX {db_table}_unique_index
            ON {db_schema}.{db_table}(match_id);
        """
        execute_sql(string_value, session=session)


def bulk_upsert_sql(
    df: pd.DataFrame,
    db_table: str,
    db_schema: str,
    on_conflict_columns: list = None,
    set_columns: list = None,
    target_columns: list = None,
    session=None,
    terminate_connection=True,
):
    """
    bulk upserts dataframe: df, to schema: dbschema and table: dbtable, if on_conflict_columns is not None,
    set_columns must also not be None, on_conflict columns must only include the primary key combinations for
    the table. set_columns would mean that the value for that primary key on conflict for the column combination
    is the new set value in the dbtable.

    inputs:
    * df: the target dataframe to be upserted to sql table
    * dbschema: the target schema
    * dbtable: the target database table
    * on_conflict_columns: the primary key columns of df and dbtable where conflict exists
    * set_columns: the set value of the columns
    * target_columns: the subset of target columns to be upserted, remaining columns null, if none by default all columns of df
    * dbsession: a db connection if exists, if not created in default

    returns: row count of df

    raises: exception for incorrect query or incorrect credentials.
    """

    try:
        if not session:
            session = create_session()

        target_columns = target_columns if target_columns else list(df.columns)
        df = convert_datetime_timestamp_cols_to_string(df)

        # for names, we need to remove any apostrophes in names
        for name in ["name", "full_name", "first", "last", "first_name", "last_name"]:
            if name in df.columns:
                df[name] = df[name].str.replace("'", "''")

        on_conflict_final_string = ""
        dataframe_cols_str_from_list = ",".join([str(elem) for elem in target_columns])

        if on_conflict_columns:
            if not set_columns:
                print("need to have set_columns, if on_conflict_columns not empty")

            on_conflict_cols_str_from_list = ",".join(
                [str(elem) for elem in on_conflict_columns]
            )
            temp_str = ""
            for column in set_columns:
                temp_str = f"{temp_str},{column}=VALUES({column})"

            on_conflict_update_str = f"{temp_str[1:]};"

            on_conflict_final_string = f"""
            ON DUPLICATE KEY UPDATE
                    {on_conflict_update_str}
            """

        value_string = (
            str([tuple(x) for x in df.to_records(index=False)])
            .replace("[", "")
            .replace("]", "")
            .replace('"', "'")
        )
        value_string = value_string.replace("None", "null")
        value_string = value_string.replace("nan", "null")
        upsert_sql_string = f"""
                INSERT INTO {db_schema}.{db_table}({dataframe_cols_str_from_list}) 
                VALUES {str(value_string)} 
                {on_conflict_final_string}"""

        execute_sql(
            upsert_sql_string,
            session=session,
            terminate_connection=terminate_connection,
        )

        n_rows = len(df)

        print(f"upserted' {str(n_rows)} rows to {db_schema}.{db_table}")
        return n_rows

    except Exception as exc:
        raise exc


def bulk_upsert_write_sql(df, dbtable, dbschema, session=None, **kwargs):
    """
    bulk upserts dataframe: df, to schema: dbschema and table: dbtable,
    if there are new values, it appends to the existing dbtable at the dbschema
    if there are conflicts, the function automatically collects the primary key of table
    and update the other values

    inputs:
    * df: the target dataframe to be upserted to sql table
    * dbschema: the target schema
    * dbtable: the target database table
    * session: a db connection if exists, if not created in default

    returns: row count of df

    raises: exception for incorrect query or incorrect credentials.
    """

    function_name = bulk_upsert_write_sql.__name__

    try:

        def mysql_upsert(table, conn, keys, data_iter):
            from sqlalchemy.dialects.mysql import insert

            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            upsert_statement = insert_statement.on_duplicate_key_update(
                # constraint=f"{table.table.name}_pk",
                {c.key: c for c in insert_statement.table.columns if not c.primary_key}
                # set_={c.key: c for c in insert_statement.values()},
            )
            conn.execute(upsert_statement)

        df.to_sql(
            name=dbtable,
            schema=dbschema,
            con=session,
            if_exists="append",
            index=False,
            chunksize=10000,
            method=mysql_upsert,
        )

        logging.info(f"upserted' {str(len(df))} rows to {dbschema}.{dbtable}")

    except Exception as exc:
        raise exc
