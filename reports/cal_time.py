from collections import defaultdict
import pytz
from datetime import datetime, time, timedelta
from dateutil import tz
import pandas as pd


class Report:
    def __init__(
        self, timezone: str, store_business_hours: pd.DataFrame, polls: pd.DataFrame
    ):
        self.timezone = timezone
        self.store_business_hours = store_business_hours
        # polls['timestamp_utc'] = polls['timestamp_utc'].dt.tz_convert(self.timezone)

        self.business_hours: dict[int:(time, time)] = self.get_business_hours_dict()
        print(polls)
        print(self.business_hours)
        # for row, data in self.polls.iterrows():  # loop over the polling df
        #     for time_slot in self.business_hours[
        #         data["day"]
        #     ]:  # loop over the business_hours dict
                # print(
                #     f"start time : {time_slot[0]}---poll {data['timestamp_utc']}---end time : {time_slot[1]}"
                # )
        #         if time_slot[0] < data['time']  < time_slot[1] :
        #             print('yes')

    def get_business_hours_dict(self) -> dict[int:(time, time)]:
        open_hours_in_day_map: dict[list] = defaultdict(list)
        for index, row in self.store_business_hours.iterrows():
            day = row["day_of_week"]
            open_hours_in_day_map[day].append(
                (datetime.strptime(row["business_start_time"], "%H:%M:%S"),
                datetime.strptime(row["business_end_time"], "%H:%M:%S")),
            ),

        return open_hours_in_day_map

    # def get_active_hours(self):
    #     print(self.polls)

    def p(self):
        print(f"time zones for stores : {self.timezone}")
        print(f"store status during day \n{self.polls}")
        print("..................................................")
        print(f"store business hours \n{self.store_business_hours}")
        print("..................................................")
