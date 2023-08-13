import os
import pandas as pd
from database.connections import get_connection
from datetime import datetime
from reports.report_status import cache

async def add_days_of_week() -> None:
    """
    Adds the days of the week to the database\n
    returns None
    """
    days_names_columns = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    days_number_columns = [0, 1, 2, 3, 4, 5, 6]
    if await check_if_table_empty("days_of_week"):
        query = f"INSERT INTO days_of_week (day_name, day_number) VALUES ($1, $2)"
        async with get_connection() as conn:
            async with conn.transaction():
                for i in zip(days_names_columns, days_number_columns):
                    await conn.execute(query, i[0], i[1])


async def add_stores_data(root_dir: os.PathLike) -> None:
    # opening closing data csv -> df
    # menu_file_path = os.path.join(root_dir, "csv_data", "Menu hours.csv")
    # menu_file_df = pd.read_csv(menu_file_path)
    # Time Zones csv -> df
    # @TODO: remove
    bq_file_path = os.path.join(
        root_dir, "csv_data", "bq-results-20230125-202210-1674678181880.csv"
    )
    bq_file_df = pd.read_csv(bq_file_path)


    columns = ["store_id", "local_time_zone"]
    if await check_if_table_empty("stores"):
        records = [tuple(x) for x in bq_file_df.values]
        async with get_connection() as conn:
            async with conn.transaction():
                await conn.copy_records_to_table(
                    "stores", records=records, columns=columns
                )

    # add stores from operations time csv ( to add missing stores)
    # stores_in_menu_csv = set(int(x[0]) for x in menu_file_df.values)
    # stores_in_bq_csv = set(int(x[0]) for x in bq_file_df.values)

    # stores_not_in_db = stores_in_menu_csv - stores_in_bq_csv
    # stores_in_bq_but_not_in_db = [(x, "America/Chicago") for x in stores_not_in_db]
    # async with get_connection() as conn:
    #     async with conn.transaction():
    #         for i in stores_in_bq_but_not_in_db:
    #             await conn.execute(
    #                 """
    #                          INSERT INTO stores (store_id,local_time_zone) values($1,$2)
    #                          ON CONFLICT (store_id) DO NOTHING 
    #                          """,
    #                 i[0],
    #                 i[1],
    #             )




async def add_business_hours(root_dir: os.PathLike):
    menu_file_path = os.path.join(root_dir, "csv_data", "Menu hours.csv")
    menu_file_df = pd.read_csv(menu_file_path)
    # id_in_menu_file = list(map(str, menu_file_df["store_id"].unique()))
    print("adding bussines hours")

    async with get_connection() as conn:
        async with conn.transaction():

            menu_file_df["start_time_local"] = pd.to_datetime(
                menu_file_df["start_time_local"], format="%H:%M:%S"
            )
            menu_file_df["end_time_local"] = pd.to_datetime(
                menu_file_df["end_time_local"], format="%H:%M:%S"
            )

            data = menu_file_df.values.tolist()
            #testing

            """INSERT INTO business_hours (  store_id, 
                                                            day_of_week_id, 
                                                            business_start_time,   
                                                            business_end_time) 
                                                            SELECT $1, $2, $3::time, $4::time
                                                            WHERE NOT EXISTS (
                                                            SELECT 1 FROM business_hours
                                                            WHERE store_id = $1 AND day_of_week_id = $2 AND business_start_time = $3 AND business_end_time = $4) """
        
            insert_query = f"""INSERT INTO business_hours (store_id, 
                                                            day_of_week_id, 
                                                            business_start_time,   
                                                            business_end_time) 
                                                            VALUES ($1, $2, $3::time, $4::time)
                                                            ON CONFLICT ON CONSTRAINT unique_business_hours DO NOTHING; """

            await conn.executemany(insert_query, data)


async def update_store_status(root_path: os.PathLike) -> None:
    """
    Updates the status of the stores in the database\n
    returns None
    """
    status_csv = pd.read_csv(os.path.join(root_path, "csv_data", "store status.csv"), parse_dates=['timestamp_utc'])
    status_csv['timestamp_utc'] = pd.to_datetime(status_csv['timestamp_utc'],errors='coerce').dt.tz_convert(None)
    status_csv['timestamp_utc'] = status_csv['timestamp_utc'].where(pd.notnull(status_csv['timestamp_utc']), pd.Timestamp.min)
    values = status_csv.values.tolist()
    # testing 
    # na_values = status_csv.isna()

    # Get the rows and columns where NaT values occur
    # rows_with_nat = na_values.any(axis=1)
    # columns_with_nat = na_values.any(axis=0)

    # Print the rows and columns with NaT values
    # print("Rows with NaT values:")
    # print(status_csv[rows_with_nat])

    # print("\nColumns with NaT values:")
    # print(status_csv.columns[columns_with_nat])
    # status_csv = status_csv.where(pd.notnull(status_csv), None)

    # print("adding store status to database")
    async with get_connection() as conn:
        async with conn.transaction():
            await conn.executemany("INSERT INTO store_status (store_id, status, timestamp_utc) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT unique_store_status DO NOTHING;",values)


async def check_if_table_empty(table_name: str) -> bool:
    """Takes in a table name\n
    returns True if table is empty else False"""

    query = f"SELECT 1 FROM {table_name} WHERE EXISTS (SELECT 1 FROM {table_name});"

    async with get_connection() as conn:
        async with conn.transaction():
            return False if await conn.fetchrow(query) else True


async def create_tables(root_dir: os.PathLike) -> None:
    """Create tables from Schema.sql \n
    Returns None"""
    schema_file = os.path.join(root_dir, "database", "schema.sql")

    with open(schema_file, "r") as schemas:
        schemas = schemas.read()
        async with get_connection() as conn:
            async with conn.transaction():
                await conn.execute(schemas)



async def on_startup() -> None:
    """
    Runs on startup to add tables to database if not exist \n
    Adds data from CSV files to database if it doesn't exist\n
    returns None
    """

    root_dir = os.path.dirname(os.path.abspath(__file__))

    # Create Tables if they don't exist
    print("crreating tables...")
    await create_tables(root_dir)

    # Fill out days_of_week table
    print("adding days_of_week...")
    await add_days_of_week()

    # Fill out stores table
    print("adding stores data...")
    await add_stores_data(root_dir)

    # Fill out store_status table
    print("adding store status...")
    await update_store_status(root_dir)

    # Fill business_hours table
    print("adding business_hours...")
    await add_business_hours(root_dir)


