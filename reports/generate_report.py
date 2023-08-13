import pandas as pd
from database.connections import get_connection
from reports.report_status import cache
from typing import Awaitable
from .cal_time import Report
import time


async def calculate_time(df, _store_id: int):
    day_map = {
        i["day_name"].capitalize(): i["day_number"] for i in await get_days_map()
    }
    store_status_timestamp = df.copy()
    store_status_timestamp["timestamp_utc"] = pd.to_datetime(
        store_status_timestamp["timestamp_utc"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )

    store_status_timestamp.loc[:, "day"] = (
        store_status_timestamp["timestamp_utc"].dt.day_name().replace(day_map)
    )
    # store_status_timestamp['timestamp_utc'] = pd.to_datetime(store_status_timestamp['timestamp_utc'], format='%Y-%m-%d %H:%M:%S')
    # print(store_status_timestamp)
    business_hours: pd.DataFrame = await get_business_hours(_store_id)
    store_timezone = await get_timezone(_store_id)
    instance = Report(store_timezone, business_hours, store_status_timestamp)
    await instance.async_init()


async def report():
    df = pd.DataFrame(await get_df())
    df.columns = ["id", "store_id", "status", "timestamp_utc"]
    store_ids_list = (int(i) for i in df["store_id"].unique().tolist())
    for (
        store_id
    ) in (
        store_ids_list
    ):  # @TODO this way only checks all stores in store_id list check if there are any other stores from other 2 csv file
        newdf = df[df["store_id"] == store_id]
        await calculate_time(newdf, store_id)
    cache["report_status"] = "complete"


async def get_days_map():
    async with get_connection() as conn:
        async with conn.transaction():
            return await conn.fetch(
                """
                             SELECT day_name,day_number FROM days_of_week ;"""
            )


async def get_business_hours(_id: int) -> Awaitable[pd.DataFrame]:
    async with get_connection() as conn:
        async with conn.transaction():
            active_time = await conn.fetch(
                f"""
                             SELECT store_id,day_of_week_id, business_start_time, business_end_time FROM business_hours WHERE store_id = {_id} ;"""
            )
        active_time_l = [
            (
                int(i["store_id"]),
                i["day_of_week_id"],
                i["business_start_time"].strftime("%H:%M:%S"),
                i["business_end_time"].strftime("%H:%M:%S"),
            )
            for i in active_time
        ]
        active_time_df = pd.DataFrame(
            active_time_l,
            columns=[
                "store_id",
                "day_of_week",
                "business_start_time",
                "business_end_time",
            ],
        )
        return active_time_df


async def get_timezone(_id):
    async with get_connection() as conn:
        async with conn.transaction():
            return (
                (
                    await conn.fetch(
                        f"""
                             SELECT local_time_zone FROM stores WHERE store_id = $1 ;""",
                        _id,
                    )
                )[0]["local_time_zone"]
                or "America/Chicago"
            )


async def get_df():
    async with get_connection() as conn:
        async with conn.transaction():
            return await conn.fetch(
                """
                            select * from store_status 
                                        order by  timestamp_utc asc;;"""
            )
