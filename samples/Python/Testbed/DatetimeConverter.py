import datetime as dt
import numpy as np
import pandas as pd

import pytz
import StaticVariables as SVbl

"""
When dealing with datetime objects, I've come across two pieces of advice with which I generally agree. 
First, always use "aware" datetime objects. And second, always work in UTC and do timezone conversion as a last step.
More specifically, as pointed out by user jarshwah on reddit, you should store datetimes in UTC and convert on display.

time stored in database is 'timestamptz', it's always stored with timezone info 
time string returned from IB server is in the format of "20201229 15:48:16" ("%Y%m%d %H:%M:%S")
time integer returned from IB server is in the format of timestamp 6465.2116464.
Use dt.datetime.fromtimestamp(timeStamp) to convert to datetime format.

PostgreSQL Database:
The timestamptz datatype is the timestamp with the time zone. The timestamptz datatype is a time zone-aware date and time data type.
PostgreSQL stores the timestamptz in UTC value. 
 - When you insert a value into a timestamptz column, PostgreSQL converts the timestamptz value into a UTC value 
and stores the UTC value in the table.
 - When you query timestamptz from the database, PostgreSQL converts the UTC value back to the time value of the 
timezone set by the database server, the user, or the current database connection.


https://howchoo.com/g/ywi5m2vkodk/working-with-datetime-objects-and-timezones-in-python
https://phili.pe/posts/timestamps-and-time-zones-in-postgresql/
https://dev.to/moijes12/converting-an-aware-datetime-object-from-one-timezone-to-another-in-python-173f
"""

timezone_syd_str = 'Australia/Sydney'
timezone_syd = pytz.timezone(timezone_syd_str)


def convert_naive_dt_to_utc(syd_naive_dt: dt.datetime):
    syd_aware_dt = timezone_syd.localize(syd_naive_dt)
    utc_dt = syd_aware_dt.astimezone(tz=pytz.utc)
    return utc_dt


def convert_aware_dt_to_utc(dt_aware: dt.datetime):
    return dt_aware.astimezone(tz=pytz.utc)


def convert_utc_dt_to_syd_naive(dt_utc: dt.datetime):
    dt_aware = dt_utc.astimezone(timezone_syd)
    dt_naive = dt_aware.replace(tzinfo=None)
    return dt_naive


def convert_aware_dt_to_syd_naive(dt_aware: dt.datetime):
    dt_naive = dt_aware.replace(tzinfo=None)
    return dt_naive


def convert_utc_dt_series_to_naive(dt_utc_series: pd.Series):
    lst = []
    for item in dt_utc_series.iteritems():
        d = item[1]
        dt_syd_naive = convert_utc_dt_to_syd_naive(d)
        lst.append(dt_syd_naive)
    ser = pd.Series(lst)
    return ser


def convert_aware_dt_series_to_naive(dt_aware_series: pd.Series):
    lst = []
    for item in dt_aware_series.iteritems():
        d = item[1]
        dt_syd_naive = convert_aware_dt_to_syd_naive(d)
        lst.append(dt_syd_naive)
    ser = pd.Series(lst)
    return ser


def convert_utc_dt_df_to_naive(df: pd.DataFrame):
    t_ser = df['time']
    t_syd_naive_ser = convert_utc_dt_series_to_naive(t_ser)
    df['time_naive'] = t_syd_naive_ser
    return df


def convert_aware_dt_df_to_naive(df: pd.DataFrame):
    t_ser = df['time']
    t_syd_naive_ser = convert_aware_dt_series_to_naive(t_ser)
    df['time_naive'] = t_syd_naive_ser

    return df


def set_dt_naive_index(df: pd.DataFrame):
    print('Set index to time_naive, and drop ''time'' column:')
    df.set_index('time_naive', inplace=True)
    df.sort_values(by='time_naive', inplace=True)
    print(df)
    return df


# ------------------- Duration & Trading Days Related Functions -----------------------


# pass in latest datetime, and return the parameters need to use in reqHistoricalData
def get_busday_str(begin_dt_naive: dt.datetime, end_dt_naive: dt.datetime):
    # Counts the number of valid days between `begindates` and
    #     `enddates`, not including the day of `enddates`.
    # If ``enddates`` specifies a date value that is earlier than the
    #     corresponding ``begindates`` date value, the count will be negative.
    weekdays = np.busday_count(begin_dt_naive.date(), end_dt_naive.date())
    return weekdays


def cut_durations_for_1day_bar(weekdays: int):
    # The max length can be requested for 1 day bar is 1 year
    # 1 year working days = 52 wks * 5 days = 260 working days
    n = weekdays // 260
    return n


def get_proper_end_dt_str(syd_dt_naive: dt.datetime):
    end_dt_naive = get_proper_end_dt(syd_dt_naive)
    end_dt_naive_str = dt.datetime.strftime(end_dt_naive, '%Y%m%d %H:%M:%S')
    return end_dt_naive_str


def get_proper_start_dt_str(syd_dt_naive: dt.datetime):
    start_dt_naive = get_proper_start_dt(syd_dt_naive)
    start_dt_naive_str = dt.datetime.strftime(start_dt_naive, '%Y%m%d %H:%M:%S')
    return start_dt_naive_str


def get_proper_end_dt(syd_dt_naive: dt.datetime):
    proper_date = get_proper_ASX_trading_dt_full_hours(syd_dt_naive)
    return dt.datetime.combine(proper_date, dt.time.max)


def get_proper_start_dt(syd_dt_naive: dt.datetime):
    proper_date = get_proper_ASX_trading_dt_full_hours(syd_dt_naive)
    return dt.datetime.combine(proper_date, dt.time.min)


def get_proper_ASX_trading_dt_full_hours(dt_naive: dt.datetime):
    """
    Returns previous trading date:
     - If date is Sat, Sun
     - If hour is between 00:00:00 (not incl) and 16:12:00 (incl)
    Returns current trading date:
     - If date is Mon - Fri and hour is bigger than 16:12:00
    """
    day = dt_naive.weekday()
    d = dt_naive.date()
    t = dt_naive.time()
    if day == 6:  # Sun: return last Fri
        proper_date = d - dt.timedelta(days=2)
    elif day == 5:  # Sat: return last Fri
        proper_date = d - dt.timedelta(days=1)
    else:  # 0: Mon - 4: Fri
        if dt.time(0, 0, 0) < t <= dt.time(14, 12):  # Pre-market or market is open
            if day == 0:  # Mon: return last Fri
                proper_date = d - dt.timedelta(days=3)
            else:  # Tue - Fri: return prev day
                proper_date = d - dt.timedelta(days=1)
        else:
            proper_date = d  # market is closed in weekdays
    return proper_date


def get_prev_trading_dt(dt_naive: dt.datetime):
    print('Get prev trading datetime for {}.'.format(dt_naive.strftime('%Y-%m-%d %H:%M:%S')))
    day = dt_naive.weekday()
    if day == 6:  # Sun: return last Fri
        prev_tr_dt = dt_naive - dt.timedelta(days=2)
    elif day == 0:  # Mon: return last Fri
        prev_tr_dt = dt_naive - dt.timedelta(days=3)
    else:  # 1 - 5, Tue - Sat: return prev day
        prev_tr_dt = dt_naive - dt.timedelta(days=1)
    return prev_tr_dt


def get_prev_trading_date(dt_naive: dt.datetime):
    day = dt_naive.weekday()
    d = dt_naive.date()
    if day == 6:  # Sun: return last Fri
        prev_tr_date = d - dt.timedelta(days=2)
    elif day == 0:  # Mon: return last Fri
        prev_tr_date = d - dt.timedelta(days=3)
    else:  # 1 - 5, Tue - Sat: return prev day
        prev_tr_date = d - dt.timedelta(days=1)
    print('Prev trading date is {}.'.format(prev_tr_date))
    return prev_tr_date


def get_prev_trading_date_for_date(date_naive: dt.date):
    day = date_naive.weekday()
    d = date_naive
    if day == 6:  # Sun: return last Fri
        prev_tr_date = d - dt.timedelta(days=2)
    elif day == 0:  # Mon: return last Fri
        prev_tr_date = d - dt.timedelta(days=3)
    else:  # 1 - 5, Tue - Sat: return prev day
        prev_tr_date = d - dt.timedelta(days=1)
    print('Prev trading date is {}.'.format(prev_tr_date))
    return prev_tr_date


def get_proper_trading_date(dt_naive: dt.datetime):
    print('Get proper trading date for {}'.format(dt_naive.date()))
    day = dt_naive.weekday()
    d = dt_naive.date()
    t = dt_naive.time()
    if day == 6:  # Sun: return last Fri
        proper_date = d - dt.timedelta(days=2)
    elif day == 5:  # Sat: return last Fri
        proper_date = d - dt.timedelta(days=1)
    else:  # 0 - 4, Mon - Fri: return the date
        proper_date = d
    return proper_date


def get_next_trading_date(dt_naive: dt.datetime):
    print('Get next trading date for {}.'.format(dt_naive.date()))
    day = dt_naive.weekday()
    d = dt_naive.date()
    if day == 5:  # Sat: return next Mon
        proper_date = d + dt.timedelta(days=2)
    elif day == 4:  # Fri: return next Mon
        proper_date = d + dt.timedelta(days=3)
    else:  # 0 - 3, 6: Mon - Thu, Sun
        proper_date = d + dt.timedelta(days=1)
    return proper_date


def get_proper_start_end_dt_utc_for_ASX_prev_trading_day(dt_utc: dt.datetime):
    dt_naive = convert_utc_dt_to_syd_naive(dt_utc)
    proper_date = get_prev_trading_date(dt_naive)
    start_naive = dt.datetime.combine(proper_date, dt.time.min)
    end_naive = dt.datetime.combine(proper_date, dt.time.max)
    start_dt_utc = convert_naive_dt_to_utc(start_naive)
    end_dt_utc = convert_naive_dt_to_utc(end_naive)
    return start_dt_utc, end_dt_utc


def get_proper_start_end_dt_utc_for_ASX_trading_day(dt_utc: dt.datetime):
    dt_naive = convert_utc_dt_to_syd_naive(dt_utc)
    proper_date = get_proper_trading_date(dt_naive)
    start_naive = dt.datetime.combine(proper_date, dt.time.min)
    end_naive = dt.datetime.combine(proper_date, dt.time.max)
    start_dt_utc = convert_naive_dt_to_utc(start_naive)
    end_dt_utc = convert_naive_dt_to_utc(end_naive)
    return start_dt_utc, end_dt_utc


def get_proper_start_end_dt_utc_for_ASX_next_trading_day(dt_utc: dt.datetime):
    dt_naive = convert_utc_dt_to_syd_naive(dt_utc)
    proper_date = get_next_trading_date(dt_naive)
    start_naive = dt.datetime.combine(proper_date, dt.time.min)
    end_naive = dt.datetime.combine(proper_date, dt.time.max)
    start_dt_utc = convert_naive_dt_to_utc(start_naive)
    end_dt_utc = convert_naive_dt_to_utc(end_naive)
    return start_dt_utc, end_dt_utc


# Test Code
def test():
    d = dt.datetime(2021, 1, 8, 10, 35, 23)
    d1 = dt.datetime(2021, 1, 8, 9, 59, 44)
    d2 = dt.datetime(2021, 1, 9, 7, 35, 23)   # Sat
    d3 = dt.datetime(2021, 1, 10, 10, 35, 23)
    d4 = dt.datetime(2021, 1, 11, 9, 59, 46)


