# Created on Fri Jun 05 2020
# 
# This file is a part of autoassets.
# autoassets is a free and open-source asset manager written by
# Aaron Arthurs ("the Author") to facilitate development and execution
# across a variety of trade strategies at the user's discretion.
# 
# Copyright (c) 2020-present, Aaron Arthurs <aajarthurs@gmail.com>
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""
Schedule helpers.
"""

import pandas as pd
from datetime import time, timedelta
from pandas.tseries.holiday import *
from pandas.tseries.offsets import CDay
from pytz import timezone

class USMarketHolidayCalendar(AbstractHolidayCalendar):
    """
    US market holiday calendar based on rules specified by:
    https://www.nyse.com/markets/hours-calendars
    """
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=sunday_to_monday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('Juneteenth National Independence Day', month=6, day=19, observance=nearest_workday),
        Holiday('Independence Day', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]
#END: USMarketHolidayCalendar

calendar_us        = USMarketHolidayCalendar()
bday_us            = CDay(calendar = calendar_us)
open_time          = time(9, 30, 0, 0, tzinfo = timezone('America/New_York'))
close_time         = time(16,15, 0, 0, tzinfo = timezone('America/New_York'))
default_start_time = time(9, 31, 0, 0, tzinfo = timezone('America/New_York'))
default_stop_time  = time(15,59, 0, 0, tzinfo = timezone('America/New_York'))

def get_session_coeff():
    """
    Return a coefficient in the range of [0,1] representing point in a cash session, where 0 indicates start of session and 1 indicates end of session.

    Returns
    -------
    float
        Session coefficient.
    """
    now_utc = pd.to_datetime('now', utc=True)
    now     = now_utc.tz_convert('America/New_York')
    if len(pd.date_range(start=now, end=now, freq=bday_us)) == 0:
        return False
    pd_open_time  = pd.to_datetime(open_time.isoformat())
    pd_close_time = pd.to_datetime(close_time.isoformat())
    pd_now_time   = pd.to_datetime(now.time().isoformat())
    return (pd_now_time - pd_open_time) / (pd_close_time - pd_open_time)
#END: get_session_coeff

def is_now_normal_market_hours():
    """
    Test if current time is within normal market hours. The difference between this function and `is_now_tradable()` is the hard open (`open_time`) and close (`close_time`). This function is normally called by broker-backends (see `autoassets/backend`) to determine when to allow trades.

    Returns
    -------
    bool
        True if within normal market hours; False otherwise.
    """
    now_utc = pd.to_datetime('now', utc=True)
    now     = now_utc.tz_convert('America/New_York')
    if len(pd.date_range(start=now, end=now, freq=bday_us)) == 0:
        return False
    now_time = now.time()
    if now_time < open_time or now_time > close_time:
        return False
    return True
#END: is_now_normal_market_hours

def is_now_tradable(start_time=None, stop_time=None):
    """
    Test if current time is within hours of trade. The difference between this function and `is_now_normal_market_hours()` is the margin of time after open (`start_time`) and before close (`stop_time`). This function is normally called by strategies (see `autoassets/strategy`) to, for example, neutralize a position just before the market closes.

    Parameters
    ----------
    start_time: datetime.time (optional)
        Set to override default opening time for trades.

    stop_time: datetime.time (optional)
        Set to override default closing time for trades.

    Returns
    -------
    bool
        True if within tradable hours; False otherwise.
    """
    now_utc = pd.to_datetime('now', utc=True)
    now     = now_utc.tz_convert('America/New_York')
    if len(pd.date_range(start=now, end=now, freq=bday_us)) == 0:
        return False
    now_time = now.time()
    selected_start_time = default_start_time if start_time is None else start_time
    selected_stop_time = default_stop_time if stop_time is None else stop_time
    if now_time < selected_start_time or now_time > selected_stop_time:
        return False
    return True
#END: is_now_tradable
