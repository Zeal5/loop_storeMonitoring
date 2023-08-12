from collections import defaultdict
from pytz import timezone
from datetime import datetime, time, timedelta
from dateutil import tz
import pandas as pd


class Report:
    def __init__(
        self, _timezone: str, store_business_hours: pd.DataFrame, polls: pd.DataFrame
    ):
        self.timezone = _timezone
        self.store_business_hours = store_business_hours
        # convert time from utc time to local time
        polls['timestamp_local'] = pd.to_datetime(polls['timestamp_utc']).dt.tz_localize('UTC').dt.tz_convert(self.timezone)
        # drop timestamp_utc column
        self.polls = polls.drop(columns= ['timestamp_utc'])    
        # add date col 
        self.polls['date_col'] = self.polls['timestamp_local'].dt.date
        self.business_hours: dict[int:(time, time)] = self.get_business_hours_dict()
        # print(self.polls)
        # print(self.business_hours)
        active_hours_dict = {}
        last_ping = {}
        business_hours_count = []
        for row, data in self.polls.iterrows():  # loop over the polling df
            for time_slot in self.business_hours[data["day"]]:  
                start_time = timezone(self.timezone).localize(time_slot[0].replace(
                    year=data["date_col"].year,
                    month=data["date_col"].month,
                    day=data["date_col"].day,
                ))
                end_time = timezone(self.timezone).localize(time_slot[1].replace(
                    year=data["date_col"].year,
                    month=data["date_col"].month,
                    day=data["date_col"].day,
                ))
                # print(f"start time : {start_time}---poll {data['timestamp_local']}---end time : {end_time}")
                key = f"{data['date_col'].day}-{data['date_col'].month}"
                if key not in active_hours_dict:
                    active_hours_dict[key] = {'active': 0, 'inactive': 0}
                if f'{start_time}-{end_time}' not in business_hours_count:
                    active_hours_dict[key]['active'] += (end_time - start_time).total_seconds() / 3600
                    business_hours_count.append(f'{start_time}-{end_time}')
                    # print(business_hours_count)
                    # print(active_hours_dict)


                if start_time <= data['timestamp_local']  <= end_time:
                    print(last_ping)
                    if data['status'] == 'inactive':
                        active_hours_dict[key]['inactive'] += (data['timestamp_local'] - (last_ping[f"{data['date_col'].day}-{data['date_col'].month}"] if last_ping else start_time)).total_seconds() / 3600
                        active_hours_dict[key]['active'] -= (data['timestamp_local'] -  (last_ping[f"{data['date_col'].day}-{data['date_col'].month}"] if last_ping else start_time)).total_seconds() / 3600
                        pass

                    last_ping[f"{data['date_col'].day}-{data['date_col'].month}"] = data['timestamp_local']
                    print(active_hours_dict)


                        

    def get_business_hours_dict(self) -> dict[int:(time, time)]:
        open_hours_in_day_map: dict[list] = defaultdict(list)
        for index, row in self.store_business_hours.iterrows():
            day = row["day_of_week"]
            open_hours_in_day_map[day].append(
                (datetime.strptime(row["business_start_time"], "%H:%M:%S"),
                datetime.strptime(row["business_end_time"], "%H:%M:%S")),
            ),

        return open_hours_in_day_map

