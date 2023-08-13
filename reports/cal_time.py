from collections import defaultdict
from pytz import timezone
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
from database.connections import get_connection
import time


class Report:
    def __init__(
        self, _timezone: str, store_business_hours: pd.DataFrame, polls: pd.DataFrame
    ):
        # assume all stores are active unless polling explicitly states otherwise
        self.timezone = _timezone
        self.store_business_hours = store_business_hours
        self.polls = polls
        # convert time from utc time to local time
        self.polls = self.clean_polls()
        self.business_hours: dict[int:(time, time)] = self.get_business_hours_dict()

    async def async_init(self):
        await self._update_db(active_hours_dict=await self.get_cal_time())

    async def get_cal_time(self) -> dict:
        active_hours_dict = {}
        last_ping = {}
        business_hours_count = []
        #loop over all the polls and get the timestamp
        for row, data in self.polls.iterrows():  
            #check if the polling time is within business hours
            for time_slot in self.business_hours[data["day"]]:
                start_time = timezone(self.timezone).localize(
                    time_slot[0].replace(
                        year=data["date_col"].year,
                        month=data["date_col"].month,
                        day=data["date_col"].day,
                    )
                )
                end_time = timezone(self.timezone).localize(
                    time_slot[1].replace(
                        year=data["date_col"].year,
                        month=data["date_col"].month,
                        day=data["date_col"].day,
                    )
                )

                key = f"{data['date_col'].day}-{data['date_col'].month}"
                # add date as key and actiive inactive time as value in dict    
                if key not in active_hours_dict:
                    active_hours_dict[key] = {"active": 0, "inactive": 0}
                if f"{start_time}-{end_time}" not in business_hours_count:
                    active_hours_dict[key]["active"] += (
                        end_time - start_time
                    ).total_seconds() / 3600
                    business_hours_count.append(f"{start_time}-{end_time}")
                
                if start_time <= data["timestamp_local"] <= end_time:
                    if data["status"] == "inactive":
                        try:
                            active_hours_dict[key]["inactive"] += (
                                data["timestamp_local"]
                                - (
                                    last_ping[
                                        f"{data['date_col'].day}-{data['date_col'].month}"
                                    ]
                                    if last_ping
                                    else start_time
                                )
                            ).total_seconds() / 3600

                            active_hours_dict[key]["active"] -= (
                                data["timestamp_local"]
                                - (
                                    last_ping[
                                        f"{data['date_col'].day}-{data['date_col'].month}"
                                    ]
                                    if last_ping
                                    else start_time
                                )
                            ).total_seconds() / 3600
                        except KeyError:
                            pass

                    last_ping[
                        f"{data['date_col'].day}-{data['date_col'].month}"
                    ] = data["timestamp_local"]

        today = datetime(
            year=2023,
            month=1,
            day=25,
            hour=23,
            minute=59,
            tzinfo=pytz.timezone(self.timezone),
        )
        last_hour_activity = {}
        last_hour_activity["active"] = 60
        last_hour_activity["inactive"] = 0
        # If the last poll from today was inactive add last hoour as inactive else active
        try:
            last_hour_status = self.polls[
                self.polls["timestamp_local"] == last_ping[f"{today.day}-{today.month}"]
            ]["status"].values[0]
            last_day_ping = last_ping[f"{today.day}-{today.month}"]
            for i, j in self.business_hours[today.weekday()]:
                localize_j = timezone(self.timezone).localize(
                    j.replace(year=today.year, month=today.month, day=today.day)
                )

                if today - timedelta(hours=1) <= last_day_ping <= localize_j:
                    if last_hour_status == "inactive":
                        last_hour_activity["inactive"] = 60
                        last_hour_activity["active"] = 0
        except KeyError:
            last_hour_activity["active"] = 60
            last_hour_activity["inactive"] = 0

        await self._update_db(last_hour_status=last_hour_activity)
        return active_hours_dict

    async def _update_db(self, active_hours_dict=None, last_hour_status=None)->None:
        """ update the active hours into the database """
        
        if last_hour_status:
            async with get_connection() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO last_hour_activity (active_time, inactive_time, store_id)
                                    VALUES ($1, $2, $3)
                                    ON CONFLICT (store_id) DO UPDATE
                                    SET active_time = EXCLUDED.active_time,
                                        inactive_time = EXCLUDED.inactive_time,
                                        store_id = EXCLUDED.store_id; """,
                        round(float(last_hour_status["active"]), 2),
                        round(float(last_hour_status["inactive"]), 2),
                        self.store_id,
                    )

                    return
        if active_hours_dict:
            today = datetime(year=2023, month=1, day=25, hour=23, minute=59)
            # print(today)
            last_week = today - timedelta(days=7)
            total_uptime = 0
            total_downtime = 0
            while last_week <= today:
                try:
                    total_uptime += active_hours_dict[
                        f"{last_week.day}-{last_week.month}"
                    ]["active"]
                    total_downtime += active_hours_dict[
                        f"{last_week.day}-{last_week.month}"
                    ]["inactive"]

                except KeyError:
                    pass
                last_week += timedelta(days=1)

            async with get_connection() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO last_week_activity (active_time, inactive_time, store_id)
                                    VALUES ($1, $2, $3)
                                    ON CONFLICT (store_id) DO UPDATE
                                    SET active_time = EXCLUDED.active_time,
                                        inactive_time = EXCLUDED.inactive_time,
                                        store_id = EXCLUDED.store_id; """,
                        round(total_uptime, 2),
                        round(total_downtime, 2),
                        self.store_id,
                    )
            try:
                if active_hours_dict[f"{today.day}-{today.month}"]:
                    today_active_hours = active_hours_dict[
                        f"{today.day}-{today.month}"
                    ]["active"]
                    today_inactive_hours = active_hours_dict[
                        f"{today.day}-{today.month}"
                    ]["inactive"]
            except KeyError:
                today_active_hours = 24
                today_inactive_hours = 24

            async with get_connection() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO last_day_activity (active_time, inactive_time, store_id)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (store_id) DO UPDATE
                                SET active_time = EXCLUDED.active_time,
                                    inactive_time = EXCLUDED.inactive_time,
                                    store_id = EXCLUDED.store_id; """,
                        round(today_active_hours, 2),
                        round(today_inactive_hours, 2),
                        self.store_id,
                    )
            print(self.store_id)
            return

    def clean_polls(self):
        self.polls["timestamp_local"] = (
            pd.to_datetime(self.polls["timestamp_utc"])
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.timezone)
        )
        # drop timestamp_utc column
        self.polls = self.polls.drop(columns=["timestamp_utc"])
        # add date col
        self.polls["date_col"] = self.polls["timestamp_local"].dt.date
        self.store_id = self.polls["store_id"].iloc[0]
        return self.polls

    def get_business_hours_dict(self) -> dict[int:(time, time)]:
        open_hours_in_day_map: dict[list] = defaultdict(list)
        for index, row in self.store_business_hours.iterrows():
            day = row["day_of_week"]
            open_hours_in_day_map[day].append(
                (
                    datetime.strptime(row["business_start_time"], "%H:%M:%S"),
                    datetime.strptime(row["business_end_time"], "%H:%M:%S"),
                ),
            ),

        return open_hours_in_day_map
