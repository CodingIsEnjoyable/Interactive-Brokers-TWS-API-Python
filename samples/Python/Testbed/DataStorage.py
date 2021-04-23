import threading
import datetime as dt

from ibapi.object_implem import Object
from ibapi.contract import Contract

"""
This class is personal defined by Rosie for the convenience to pass properties for 
tick data returned by IB.
"""


class ContractCls(Contract):
    def __init__(self):
        Contract.__init__(self)
        self.earliest_avbl_dt = dt.datetime


class TickLastData(Object):
    def __init__(self, date_time: dt.datetime, price: float, volume: int, exchange: str,
                 past_limit=False, unreported=False, special_conditions=''):
        self.time_stamp = date_time
        self.price = price
        self.volume = volume
        self.past_limit = past_limit
        self.unreported = unreported
        self.special_conditions = special_conditions


class EventClass(threading.Event):
    def __init__(self):
        self.event = threading.Event()
        self.event_id = -1
        self.event_name = ''
        self.reqId = -1


"""
Important class to link reqId with important BarData information returned by IB.
"""


class ReqIdBarDataCls(Object):
    def __init__(self, reqId: int, bar_earliest_time_returned_utc: dt.datetime,
                 bar_latest_time_utc: dt.datetime, bar_time_span: str, bar_volume=0):
        self.reqId = reqId
        self.bar_earliest_time_utc = bar_earliest_time_returned_utc
        self.bar_latest_time_utc = bar_latest_time_utc
        self.bar_time_span = bar_time_span
        self.bar_volume = bar_volume


"""
Important class to link reqId with important TickLastData information returned by IB.
TickLastData class is a personal defined class by Rosie to store all information returned
by IB related to tick last data and handled by tickByTickAllLast function in EWrapper.
"""


class ReqIdTickLastDataCls(Object):
    def __init__(self, reqId: int, tick_latest_dt_naive: dt.datetime, num_ticks_returned=0):
        self.reqId = reqId
        self.tick_latest_dt_naive = tick_latest_dt_naive
        self.num_ticks_returned = num_ticks_returned


class ReqIdCls(Object):
    def __init__(self, reqId: int, contract: Contract):
        self.reqId = reqId
        self.contract = contract
        self.datetime_now = dt.datetime.now()


class ShareSplit(Object):
    def __init__(self, date_aware: dt.date, split_ratio: float):
        self.date = date_aware
        self.number = split_ratio


class ShareFloat(Object):
    def __init__(self, date_aware: dt.date, share_float: float):
        self.date = date_aware
        self.number = share_float


class ShareIssued(Object):
    def __init__(self, date_aware: dt.date, share_issued: float):
        self.date = date_aware
        self.number = share_issued


class Organization(Object):
    def __init__(self, org_id: float, name: str):
        self.org_id = org_id
        self.name = name
        self.mkt_cap = 0.0
        self.share_issued = None
        self.share_float = None
        self.share_split = None


# A Pacing Violation1 occurs whenever one or more of the following restrictions is not observed:
# Making identical historical data requests within 15 seconds.
# No more than 1 tick-by-tick request can be made for the same instrument within 15 seconds.
# Making six or more historical data requests for the same Contract, Exchange and Tick Type within 2 seconds.
# Making more than 60 requests within any 10 minute period.
#
# Duration	            Allowed Bar Sizes
# 60 S	                1 sec - 1 mins
# 120 S	                1 sec - 2 mins
# 1800 S (30 mins)	    1 sec - 30 mins
# 3600 S (1 hr)	        5 secs - 1 hr
# 14400 S (4hr)	        10 secs - 3 hrs
# 28800 S (8 hrs)	    30 secs - 8 hrs
# 1 D	                1 min - 1 day
# 2 D	                2 mins - 1 day
# 1 W	                3 mins - 1 week
# 1 M	                30 mins - 1 month
# 1 Y	                1 day - 1 month

# The other historical data limitations listed are general limitations for all trading platforms:
# Bars whose size is 30 seconds or less older than six months
# Expired futures data older than two years counting from the future's expiration date.
# Expired options, FOPs, warrants and structured products.
# End of Day (EOD) data for options, FOPs, warrants and structured products.
# Data for expired future spreads
# Data for securities which are no longer trading.
# Native historical data for combos. Historical data is not stored in the IB database separately for combos. combo historical data in TWS or the API is the sum of data from the legs.
# Historical data for securities which move to a new exchange will often not be available prior to the time of the move.
"""
class reqFrequencyCtrl(Object):
    def __init__(self):
        self.barsize_duration_dict = {
            '1 secs': '1800 S',  # 1 sec bar the max duration can request is 30 mins
            '5 secs': '3600 S',  # 1 hr
            '10 secs': '14400 S',  # 4 hrs
            '30 secs': '28800 S',  # 8 hrs
            '1 mins': '1 D',
            '5 mins': '1 W',
            '15 mins': '2 W',
            '30 mins': '1 M',
            '1 hr': '2 M',
            '2 hrs': '3 M',
            '3 hrs': '4 M',
            '4 hrs': '6 M',
            '1 day': '1 Y'}
        # No more than 1 tick-by-tick request can be made for the same instrument within 15 seconds.
        # Do NOT Making identical historical data requests within 15 seconds.
        self.interval_same_hist_tick_req = [1, '15 secs']
        self.interval_same_contract_hist_req = [6,
                                                '2 secs']  # Making six or more historical data requests for the same Contract, Exchange and Tick Type within 2 seconds.
        self.interval_all_req = [60, '10 mins']  # # Making more than 60 requests within any 10 minute period.
        self.interval_same_contract_tick_req = [1, '15 secs']
"""
