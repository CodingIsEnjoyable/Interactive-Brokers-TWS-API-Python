import pandas as pd
import numpy as np
import datetime as dt
import StaticVariables as StcVbl
import DatabaseWriter as DBWrt
import DataStorage as DStrg

import psycopg2

import DatetimeConverter as DTCvt

from ibapi.contract import Contract, ContractDetails


def fetch_all_asx_reqid_contract_dtls(db_connection, reqId_start: int):
    try:
        db_contracts_dict = fetch_all_asx_active_contr(db_connection)
        reqId_contrdtls_dict = convert_contracts_to_reqId_contrdtls(reqId_start, db_contracts_dict)
        print("{} active contracts are loaded from DB.".format(len(reqId_contrdtls_dict)))
        return reqId_contrdtls_dict
    except Exception as e:
        print(e)


def fetch_all_asx_active_common_contr(db_connection):
    try:
        load_query_str = "SELECT * FROM contract WHERE primary_exchange = 'ASX' AND is_active = True AND stock_type = 'COMMON'"
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(load_query_str)
        contr_dict = cur.fetchall()  # fetch all data returned and store them in a dictionary
        keyed_contr_dict = {}
        for contra in contr_dict:
            contract = Contract()
            contract.conId = contra['contract_id']
            contract.symbol = contra['symbol']
            contract.currency = contra['currency']
            contract.secType = contra['security_type']
            contract.primaryExchange = contra['primary_exchange']
            contract.exchange = contra['exchange']

            keyed_contr_dict[contract.conId] = contract
        print("{} common active contracts are fetched from DB.".format(len(keyed_contr_dict)))
        return keyed_contr_dict
    except Exception as e:
        print(e)


def fetch_all_asx_active_contr(db_connection):
    try:
        load_query_str = "SELECT * FROM contract WHERE primary_exchange = 'ASX' AND is_active = True"
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(load_query_str)
        contr_dict = cur.fetchall()  # fetch all data returned and store them in a dictionary
        keyed_contr_dict = {}
        for contra in contr_dict:
            contract = Contract()
            contract.conId = contra['contract_id']
            contract.symbol = contra['symbol']
            contract.currency = contra['currency']
            contract.secType = contra['security_type']
            contract.primaryExchange = contra['primary_exchange']
            contract.exchange = contra['exchange']

            keyed_contr_dict[contract.conId] = contract
        print("{} active contracts are fetched from DB.".format(len(keyed_contr_dict)))
        return keyed_contr_dict
    except Exception as e:
        print(e)


def fetch_all_asx_active_contr_to_df(db_connection):
    try:
        query = "SELECT * FROM contract WHERE primary_exchange = 'ASX' AND is_active = True"
        df = pd.read_sql_query(con=db_connection, sql=query)
        df.sort_values(by=['symbol'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        # print(df.head())
        if len(df) > 0:
            for i in range(len(df)):
                contract = Contract()
                contract.conId = df.loc[i, 'contract_id'].item()  # Important: convert int64 to int
                contract.symbol = df.loc[i, 'symbol']
                contract.currency = df.loc[i, 'currency']
                contract.secType = df.loc[i, 'security_type']
                contract.primaryExchange = df.loc[i, 'primary_exchange']
                contract.exchange = df.loc[i, 'exchange']
                df.loc[i, 'contract'] = contract
            print("{} active contracts are fetched from DB.".format(len(df)))
            print(df.info())
            print(df.head())
            print(df.tail())
            return df
        else:
            return None
    except Exception as e:
        print(e)


def convert_db_contract_dict_to_contract_dict(db_contract_dict: {}):
    keyed_contr_dict = {}
    for contra in db_contract_dict:
        contract = Contract()
        contract.conId = contra['contract_id']
        contract.symbol = contra['symbol']
        contract.currency = contra['currency']
        contract.secType = contra['security_type']
        contract.primaryExchange = contra['primary_exchange']
        contract.exchange = contra['exchange']

        keyed_contr_dict[contract.conId] = contract
    return keyed_contr_dict


def select_all_contracts_for_primEX(db_connection, primary_exchange: str):
    try:
        query = 'SELECT * FROM contract WHERE primary_exchange=%s'
        values = [primary_exchange]
        df = pd.read_sql_query(query, db_connection, params=values)
        df.sort_values(by=['symbol'], inplace=True)
        print(df)
        return df
    except Exception as e:
        print(e)


def convert_contracts_to_reqId_contrdtls(reqId_start: int, db_contracts_dict: {}):
    reqId_contrdtls_dict = {}
    for key in db_contracts_dict:
        contrdtls = ContractDetails()
        contrdtls.contract = db_contracts_dict[key]
        reqId_contrdtls_dict[reqId_start] = contrdtls
        reqId_start += 1
    return reqId_contrdtls_dict


def conditional_fetch_contracts(db_connection, conditions: str):
    try:
        load_query_str = 'SELECT * FROM contract WHERE is_active = True AND ' + conditions
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(load_query_str)
        data_dict = cur.fetchall()  # fetch all data returned and store them in a dictionary
        keyed_dict = convert_db_contract_dict_to_contract_dict(data_dict)
        print("{} active contracts are fetched from contract table in DB.".format(len(keyed_dict)))
        return keyed_dict
    except Exception as e:
        print(e)


def is_contract_exists_in_bar(db_connection, symbol: str, contract_id: int):
    """
    Check existence functions
    Reference:
    http://zetcode.com/python/psycopg2/
    """
    exists_query_str = "SELECT EXISTS(SELECT * FROM bar WHERE symbol = %s AND contract_id = %s);"
    values = [symbol, contract_id]
    cur = db_connection.cursor()
    try:
        cur.execute(exists_query_str, values)
        e = cur.fetchone()[0]
        if e:
            print("\nContract exists in bar table in DB. Symbol: {}, Contract_id: {}.".format(
                symbol, contract_id))
        else:
            print("\nContract does NOT exist in bar table in DB. Symbol: {}, Contract_id: {}.".format(
                symbol, contract_id))
        return e
    except Exception as e:
        print(e)


def check_contr_exist(db_connection, symbol: str):
    est = False
    data_dict = select_contract_by_symbol(db_connection, symbol)
    if len(data_dict) == 1:
        print("One contract exists in database. symbol={}".format(symbol))
        est = True
    elif len(data_dict) > 1:
        print("WARNING: {} CONTRACTS EXIST IN DATABASE!!! symbol={}".format(len(data_dict), symbol))
        est = True
    elif len(data_dict) == 0:
        print("Contract does NOT exist in database. symbol={}".format(symbol))
    return est


# The default cursor retrieves the data in a tuple of tuples. With a dictionary cursor, the data
# is sent in a form of Python dictionaries. We can then refer to the data by their column names.
# The data is accessed by the column names. The column names are folded to lowercase in PostgreSQL (unless quoted)
# and are case sensitive. Therefore, we have to provide the column names in lowercase.
def select_contract_by_symbol(db_connection, symbol: str):
    query = "SELECT * FROM contract WHERE symbol = %s"
    value = [symbol, ]
    try:
        df = pd.read_sql_query(con=db_connection, sql=query, params=value)
        print(df)

        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, value)
        print("Selected data from contract in DB. symbol={}, {} rows of data returned.".format(symbol, cur.rowcount))
        data_dict = cur.fetchall()
        for row in data_dict:
            print(
                f"Contract fetched: {row['contract_id']}, {row['symbol']}, {row['full_name']}, {row['security_type']} ")
        return data_dict
    except Exception as e:
        print(e)


def select_common_contr_by_symbol(db_connection, symbol: str):
    query = "SELECT * FROM contract WHERE symbol = %s AND stock_type = 'COMMON'"
    value = [symbol]
    try:
        contr_df = pd.read_sql_query(con=db_connection, sql=query, params=value)
        # print(contr_df)
        return contr_df
    except Exception as e:
        print(e)


def select_common_contr_by_conId(db_connection, contract_id: int):
    query = "SELECT * FROM contract WHERE contract_id = %s AND stock_type = 'COMMON'"
    values = [contract_id]
    try:
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, values)
        data_dict = cur.fetchall()
        for row in data_dict:
            print(
                f"Select contract by Id from DB: ConId: {row['contract_id']}, Symbol: {row['symbol']}, SecurityType: {row['security_type']}")
        if bool(data_dict):
            contract = Contract()
            # contract = DStrg.Contract(data_dict[0]['earliest_avbl_dt'])
            contract.conId = data_dict[0]['contract_id']
            contract.symbol = data_dict[0]['symbol']
            contract.secType = data_dict[0]['security_type']
            contract.primaryExchange = data_dict[0]['primary_exchange']
            contract.currency = data_dict[0]['currency']
            contract.exchange = data_dict[0]['exchange']
            return contract
        else:
            print('No contract found for id: {}'.format(contract_id))
    except Exception as e:
        print(e)


def select_active_contr_by_conId(db_connection, contract_id: int):
    query = "SELECT * FROM contract WHERE contract_id = %s AND is_active = TRUE"
    values = [contract_id]
    try:
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, values)
        data_dict = cur.fetchall()
        for row in data_dict:
            print(
                f"Select contract by Id from DB: ConId: {row['contract_id']}, Symbol: {row['symbol']}, SecurityType: {row['security_type']}")
        if bool(data_dict):
            contract = Contract()
            contract.conId = data_dict[0]['contract_id']
            contract.symbol = data_dict[0]['symbol']
            contract.secType = data_dict[0]['security_type']
            contract.primaryExchange = data_dict[0]['primary_exchange']
            contract.currency = data_dict[0]['currency']
            contract.exchange = data_dict[0]['exchange']
            contr_cls = DStrg.ContractCls(contract, data_dict[0]['earliest_avbl_dt'])
            return contract
        else:
            print('No contract found for id: {}'.format(contract_id))
    except Exception as e:
        print(e)


def select_active_contr_cls_by_conId(db_connection, contract_id: int):
    query = "SELECT * FROM contract WHERE contract_id = %s AND is_active = TRUE"
    values = [contract_id]
    try:
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, values)
        data_dict = cur.fetchall()
        for row in data_dict:
            print(
                f"Select contract by Id from DB: ConId: {row['contract_id']}, Symbol: {row['symbol']}, SecurityType: {row['security_type']}")
        if bool(data_dict):
            contr_cls = DStrg.ContractCls()
            contr_cls.earliest_avbl_dt = data_dict[0]['earliest_avbl_dt']
            contr_cls.conId = data_dict[0]['contract_id']
            contr_cls.symbol = data_dict[0]['symbol']
            contr_cls.secType = data_dict[0]['security_type']
            contr_cls.primaryExchange = data_dict[0]['primary_exchange']
            contr_cls.currency = data_dict[0]['currency']
            contr_cls.exchange = data_dict[0]['exchange']
            return contr_cls
        else:
            print('No contract found for id: {}'.format(contract_id))
    except Exception as e:
        print(e)


def select_contracts_earliest_avbl_dt_is_null(db_connection):
    try:
        query = 'SELECT * FROM contract WHERE earliest_avbl_dt is null'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def select_earliest_avbl_dt_for_conid(db_connection, contract_id: int):
    try:
        query = 'SELECT contract_id, earliest_avbl_dt FROM contract WHERE contract_id = %s AND earliest_avbl_dt is not null'
        values = [contract_id]
        df = pd.read_sql_query(query, db_connection, params=values)
        print(df)
        return df
    except Exception as e:
        print(e)


def select_all_reits_contracts(db_connection):
    query = "select * from contract where stock_type = 'REIT'"
    try:
        df = pd.read_sql_query(con=db_connection, sql=query)
        print(df)
        return df
    except Exception as e:
        print(e)


def select_all_gold_mining_contracts(db_connection):
    query = "select * from contract where sub_category = 'Gold Mining'"
    try:
        df = pd.read_sql_query(con=db_connection, sql=query)
        print(df)
        return df
    except Exception as e:
        print(e)


def select_all_sub_categories_fr_contracts(db_connection):
    """
    IB's ASX industry categories are clarified as: Sector => Category => Sub_Category
    """
    query = 'SELECT DISTINCT sector, category, sub_category FROM contract ORDER BY sector, category'
    try:
        df = pd.read_sql_query(con=db_connection, sql=query)
        print(df)
        return df
    except Exception as e:
        print(e)


# ============================ Bar Table Related Database Functions =============================

def is_1day_bar_exists(db_connection, contract: Contract, dt_utc: dt.date, duration='1 day'):
    try:
        query = "SELECT EXISTS(SELECT * FROM bar " \
                + "WHERE symbol=%s AND contract_id=%s AND time_span=%s AND time=%s);"
        values = [contract.symbol, contract.conId, duration, dt_utc]
        cur = db_connection.cursor()
        cur.execute(query, values)
        e = cur.fetchone()[0]
        if e:
            print("\nBar exists in bar table in DB. Symbol: {}, Contract_id: {}, Duration: {}, Datetime: {}.".format(
                contract.symbol, contract.conId, duration, dt_utc))
        else:
            print(
                "\nBar does NOT exist in bar table in DB. Symbol: {}, Contract_id: {}, Duration: {}, Datetime: {}.".format(
                    contract.symbol, contract.conId, duration, dt_utc))
        return e
    except Exception as e:
        print(e)


def select_distinct_bars(db_connection, bar_size: str, start_dt_aware: dt.datetime, end_dt_aware: dt.datetime):
    try:
        query = 'SELECT DISTINCT contract_id FROM bar WHERE time_span=%s AND time>=%s AND time<=%s'
        values = [bar_size, start_dt_aware, end_dt_aware]
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, values)
        data_dict = cur.fetchall()
        print('There are {} distinct {} bars in DB.'.format(len(data_dict), bar_size))
        return len(data_dict)
    except Exception as e:
        print(e)


# This query is specific to ONLY Timescale DB
def get_last_first_bar(db_connection, symbol: str, contract_id: int, time_span: str):
    """
    TimeScale DB syntax: Fist and Last
    https://docs.timescale.com/latest/using-timescaledb/reading-data#select

    SELECT symbol, contract_id, first(time, time), last(time, time)
    FROM bar
    WHERE symbol='CBA' AND contract_id = 4036818
    GROUP BY symbol, contract_id;

    Data Returned:
    "CBA"	4036818	"2020-06-26 00:00:00+10"	"2020-12-24 00:00:00+11"
    """
    try:
        query = 'SELECT symbol, contract_id, time_span, first(time, time), last(time, time) ' + \
                'FROM bar WHERE symbol = %s AND contract_id = %s AND time_span = %s ' + \
                'GROUP BY symbol, contract_id, time_span;'
        values = [symbol, contract_id, time_span]
        cur = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query, values)
        first_last_bar_dict = cur.fetchall()
        row = first_last_bar_dict[0]
        first_dt_aware = row['first']
        last_dt_aware = row['last']
        first_dt_utc = DTCvt.convert_aware_dt_to_utc(first_dt_aware)
        last_dt_utc = DTCvt.convert_aware_dt_to_utc(last_dt_aware)
        print("Get First and Last time in DB. Symbol: {}. In DB: Earliest time: {}, Latest time: {}".format(
            symbol, first_dt_aware, last_dt_aware))
        return first_dt_utc, last_dt_utc
    except Exception as e:
        print(e)


def get_all_bars_prev_close(db_connection, time_span: str):
    try:
        query = 'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                'FROM bar WHERE time_span=%s'
        values = [time_span]
        df = pd.read_sql_query(query, db_connection, params=values)
        df.dropna(subset=['prev_close'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        print(df)
        return df
    except Exception as e:
        print(e)


def select_bars_w_prev_close_btw_dt(db_connection, time_span: str,
                                    dt_utc_start: dt.datetime, dt_utc_end: dt.datetime):
    try:
        query = 'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                'FROM bar ' \
                'WHERE time_span=%s ' \
                'AND time BETWEEN %s AND %s'
        values = [time_span, dt_utc_start, dt_utc_end]
        df = pd.read_sql_query(query, db_connection, params=values)
        df.dropna(subset=['prev_close'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        print('\nSelecting {} bars btw {} and {}, include prev_close price:'.format(
            time_span,
            DTCvt.convert_utc_dt_to_syd_naive(dt_utc_start).strftime('%Y-%m-%d %H:%M:%S'),
            DTCvt.convert_utc_dt_to_syd_naive(dt_utc_end).strftime('%Y-%m-%d %H:%M:%S')))
        print(df[['time', 'open', 'close', 'volume', 'trades', 'prev_close']].describe())
        return df
    except Exception as e:
        print(e)


def get_all_bars_with_prev_close_with_conditions(db_connection, time_span: str,
                                                 dt_utc_start: dt.datetime, dt_utc_end: dt.datetime,
                                                 open_price_low: float, open_price_high: float,
                                                 volume_low: float, volume_high: float
                                                 ):
    try:
        query = 'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                'FROM bar ' \
                'WHERE time_span=%s ' \
                'AND time BETWEEN %s AND %s ' \
                'AND open BETWEEN %s AND %s ' \
                'AND volume BETWEEN %s AND %s'
        values = [time_span, dt_utc_start, dt_utc_end, open_price_low, open_price_high, volume_low, volume_high]
        df = pd.read_sql_query(query, db_connection, params=values)
        df.dropna(subset=['prev_close'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        print(
            '\nGet all {} bars, Time btw {} and {}, Open Price btw {} and {}, Volume btw {} and {}, include prev_close price.'.format(
                time_span,
                DTCvt.convert_utc_dt_to_syd_naive(dt_utc_start), DTCvt.convert_utc_dt_to_syd_naive(dt_utc_end),
                open_price_low, open_price_high, volume_low, volume_high))
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_bars_with_condition(db_connection, time_span: str,
                                dt_utc_start: dt.datetime, dt_utc_end: dt.datetime):
    try:
        query = 'SELECT * FROM bar WHERE time_span = %s AND time BETWEEN %s AND %s'
        values = [time_span, dt_utc_start, dt_utc_end]
        bars_df = pd.read_sql_query(query, db_connection, params=values)
        print('\nGet all {} bars, Time btw {} and {}'
              .format(time_span,
                      DTCvt.convert_utc_dt_to_syd_naive(dt_utc_start),
                      DTCvt.convert_utc_dt_to_syd_naive(dt_utc_end)))
        print(bars_df)
        return bars_df
    except Exception as e:
        print(e)


def get_bars_prev_close_for_contract(db_connection, time_span: str, contr: Contract):
    try:
        query = 'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                + 'FROM bar WHERE symbol=%s AND contract_id=%s AND time_span=%s'
        values = [contr.symbol, contr.conId, time_span]
        bars_df = pd.read_sql_query(query, db_connection, params=values)
        print(bars_df)
        return bars_df
    except Exception as e:
        print(e)


def get_all_gapper_bars(db_connection, time_span: str, lowest_open_price: float, gap_pct: float, dt_start: dt.datetime):
    try:
        query = 'WITH bar_prev_close AS(' \
                'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                'FROM bar WHERE time_span=%s AND time >= %s AND open >= %s' \
                '), bar_gap_pct AS(' \
                'SELECT *, ((open - prev_close)/prev_close) gap_pct ' \
                'FROM bar_prev_close' \
                ') SELECT * FROM bar_gap_pct ' \
                'WHERE gap_pct>=%s'
        values = [time_span, dt_start, lowest_open_price, gap_pct]
        bars_df = pd.read_sql_query(query, db_connection, params=values)
        df_naive = DTCvt.convert_utc_dt_df_to_naive(bars_df)
        print('Gapper bars:\n', df_naive)
        return df_naive
    except Exception as e:
        print(e)


def get_pct_bars(db_connection, time_span: str):
    try:
        query = 'WITH bar_prev_close AS(' \
                'SELECT *, LAG(close, 1) OVER(PARTITION BY contract_id ORDER BY time) prev_close ' \
                'FROM bar WHERE time_span=%s' \
                '), bar_pct AS(' \
                'SELECT *, ' \
                '((open - prev_close)/prev_close) gap_pct, ' \
                '((close-open)/open) gain_pct, ' \
                '((high-_open)/open) high_open_pct, ' \
                '((low-_open)/open) low_open_pct, ' \
                '((high-low)/low) volatility_pct ' \
                'FROM bar_prev_close' \
                ') SELECT * FROM bar_pct '
        values = [time_span]
        bars_df = pd.read_sql_query(query, db_connection, params=values)
        print(bars_df)
        return bars_df
    except Exception as e:
        print(e)


def select_bars_btw_dt(db_connection, time_span: str, start_dt_naive: dt.datetime, end_dt_naive: dt.datetime):
    query = 'SELECT * FROM bar WHERE time_span = %s AND time BETWEEN %s AND %s ORDER BY time'
    values = [time_span, DTCvt.convert_naive_dt_to_utc(start_dt_naive), DTCvt.convert_naive_dt_to_utc(end_dt_naive)]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        if len(df) > 0:
            df_time_naive = DTCvt.convert_utc_dt_df_to_naive(df)
            print('\nSelected {} {} bars btw {} and {} from DB:'.format(
                len(df), time_span, start_dt_naive, end_dt_naive))
            print(df_time_naive)
            return df_time_naive
        print('No {} bars are selected from DB btw {} and {}!!!'.format(time_span, start_dt_naive, end_dt_naive))
        return None
    except Exception as e:
        print(e)


def select_bars_btw_dt_for_conid(db_connection, time_span: str, contract_id: int, start_dt_utc: dt.datetime,
                                 end_dt_utc: dt.datetime):
    query = 'SELECT * FROM bar WHERE time_span = %s AND contract_id = %s AND time BETWEEN %s AND %s ORDER BY time'
    values = [time_span, contract_id, start_dt_utc, end_dt_utc]
    dt_naive = [DTCvt.convert_utc_dt_to_syd_naive(start_dt_utc), DTCvt.convert_utc_dt_to_syd_naive(end_dt_utc)]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        if len(df) > 0:
            df_naive = DTCvt.convert_utc_dt_df_to_naive(df)
            print('Selected {} {} bars for {} btw {} and {} from DB:'
                  .format(len(df), time_span, df_naive.loc[0, 'symbol'], dt_naive[0], dt_naive[1]))
            print(df_naive[['time_naive', 'symbol', 'open', 'close', 'trades', 'volume']])
            return df_naive
        print('No {} bars are selected from DB btw {} and {}!!!'.format(time_span, dt_naive[0], dt_naive[1]))
        return None
    except Exception as e:
        print(e)


def select_all_bars_for_contract(db_connection, contract_id: int, time_span_str: str):
    query = 'SELECT * FROM bar WHERE contract_id = %s AND time_span = %s'
    values = [contract_id, time_span_str]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        df_naive = DTCvt.convert_utc_dt_df_to_naive(df)
        print('\nSelect all {} bars for {} from DB:'.format(time_span_str, df_naive.loc[0, 'symbol']))
        print(df_naive)
        return df_naive
    except Exception as e:
        print(e)


# ------------------------- Functions relates to processing Auction Data ---------------------------


def select_pre_aft_auctions_for_contract_btw_dt(db_connection, contract: Contract, start_dt_utc: dt.datetime,
                                                end_dt_utc: dt.datetime):
    query = 'SELECT * FROM auction WHERE contract_id = %s AND time BETWEEN %s AND %s'
    values = [contract.conId, start_dt_utc, end_dt_utc]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        df_naive = DTCvt.convert_utc_dt_df_to_naive(df)
        print('Selected {} PRE & AFTER AUCTION data for {} btw {} and {} from DB:'.format(
            len(df), contract.symbol, start_dt_utc, end_dt_utc))
        print(df_naive[['auc_type', 'symbol', 'time_naive', 'price', 'volume', 'trades']])
        return df_naive
    except Exception as e:
        print(e)


# ------------------------- Functions relates to processing Ticks ---------------------------


def select_first_last_tick_for_all_in_date(db_connection, dt_naive: dt.datetime):
    start_utc = DTCvt.convert_naive_dt_to_utc(dt.datetime.combine(dt_naive.date(), dt.time(0, 0, 0)))
    end_utc = DTCvt.convert_naive_dt_to_utc(dt.datetime.combine(dt_naive.date(), dt.time(24, 0, 0)))
    query = 'SELECT symbol, contract_id, first(time, time), last(time, time) ' \
            'FROM tick WHERE time BETWEEN %s AND %s GROUP BY symbol, contract_id ORDER BY first'
    values = [start_utc, end_utc]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        print('\nSelect first and last on {} for all contracts from tick table in DB:'.format(dt_naive.date()))
        print(df)
        return df
    except Exception as e:
        print(e)


def select_first_last_tick_ASX_for_conid_in_date(db_connection, contract: Contract, dt_naive: dt.datetime):
    """
        first() and last() syntax is only Timescale specific functions, it cannot be used on postgresql.
        query = 'SELECT symbol, contract_id, first(time, time), last(time, time) ' \
                'FROM tick WHERE contract_id = {} AND time BETWEEN {} AND {} ' \
                'GROUP BY symbol, contract_id'

        Can also use generic Postgresql syntax as below:
    """
    start_utc = DTCvt.convert_naive_dt_to_utc(dt.datetime.combine(dt_naive.date(), dt.time.min))
    end_utc = DTCvt.convert_naive_dt_to_utc(dt.datetime.combine(dt_naive.date(), dt.time.max))

    query_first = "SELECT * FROM tick WHERE time BETWEEN %s AND %s AND contract_id = %s AND exchange = 'ASX' ORDER BY time ASC LIMIT 1"
    query_last = "SELECT * FROM tick WHERE time BETWEEN %s AND %s AND contract_id = %s AND exchange = 'ASX' ORDER BY time DESC LIMIT 1"
    values = [start_utc, end_utc, contract.conId]
    try:
        print('\nSelecting first and last ticks on {} from DB for {}:'.format(dt_naive.date(), contract.symbol))
        first_df = pd.read_sql_query(sql=query_first, con=db_connection, params=values)
        last_df = pd.read_sql_query(sql=query_last, con=db_connection, params=values)
        if not first_df.empty:
            first_df = DTCvt.convert_utc_dt_df_to_naive(first_df)
            print('First tick. Time: {}, price: {}, vol: {}, exchange: {}, symbol: {}'.format(
                first_df.loc[0, 'time_naive'], first_df.loc[0, 'price'], first_df.loc[0, 'volume'],
                first_df.loc[0, 'exchange'], last_df.loc[0, 'symbol']))
        else:
            print('No First tick is selected for {} on {} from DB!!!!!!'.format(contract.symbol, dt_naive.date()))
        if not last_df.empty:
            last_df = DTCvt.convert_utc_dt_df_to_naive(last_df)
            print('Last tick. Time: {}, price: {}, vol: {}, exchange: {}, symbol: {}'.format(
                last_df.loc[0, 'time_naive'], last_df.loc[0, 'price'], last_df.loc[0, 'volume'],
                last_df.loc[0, 'exchange'], last_df.loc[0, 'symbol']))
        else:
            print('No Last tick is selected for {} on {} from DB!!!!!!'.format(contract.symbol, dt_naive.date()))
        return first_df, last_df
    except Exception as e:
        print('Exception: {}!!!'.format(e))


def select_max_min_tick_btw_dt(db_connection, start_dt_naive: dt.datetime, end_dt_naive: dt.datetime):
    """
        SELECT symbol, date(time), min(time), max(time) from tick
        where time between '2021-3-17 00:00:00' and '2021-3-18 23:59:59'
        group by symbol, date(time)
        order by symbol
    """
    start_dt_utc = DTCvt.convert_naive_dt_to_utc(start_dt_naive)
    end_dt_utc = DTCvt.convert_naive_dt_to_utc(end_dt_naive)
    query = 'SELECT symbol, date(time), min(time), max(time) FROM tick WHERE time between {} and {} ' \
            'GROUP BY symbol, date(time) ORDER BY symbol'
    values = [start_dt_utc, end_dt_utc]
    try:
        print('\nSelecting min and max ticks for all contracts btw {} and {}:'.format(start_dt_naive, end_dt_naive))
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        return df
    except Exception as e:
        print('select_max_min_btw_dt Exception: {}!!!'.format(e))


def select_max_min_tick_for_contr_btw_dt(db_connection, contract: Contract, start_dt_naive: dt.datetime, end_dt_naive: dt.datetime):
    """
        SELECT symbol, date(time), min(time), max(time) from tick
        where time between '2021-3-17 00:00:00' and '2021-3-18 23:59:59'
        group by symbol, date(time)
        order by symbol
    """
    start_dt_utc = DTCvt.convert_naive_dt_to_utc(start_dt_naive)
    end_dt_utc = DTCvt.convert_naive_dt_to_utc(end_dt_naive)
    query = 'SELECT symbol, date(time), min(time), max(time) FROM tick ' \
            'WHERE contract_id = {} AND time between {} and {} ' \
            'GROUP BY symbol, date(time) ORDER BY symbol'
    values = [contract.conId, start_dt_utc, end_dt_utc]
    try:
        print('\nSelecting min and max ticks for {} btw {} and {}:'.format(contract.symbol, start_dt_naive, end_dt_naive))
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        return df
    except Exception as e:
        print('select_max_min_for_contr_btw_dt Exception: {}!!!'.format(e))


def select_ticks_at_price_btw_dt(db_connection, contract: Contract, price: float,
                                 dt_start_utc: dt.datetime, dt_end_utc: dt.datetime):
    query = "SELECT * FROM tick WHERE time BETWEEN %s AND %s AND contract_id = %s AND price = %s AND exchange = 'ASX' ORDER BY time"
    values = [dt_start_utc, dt_end_utc, contract.conId, price]
    try:
        df = pd.read_sql_query(sql=query, con=db_connection, params=values)
        if not df.empty:
            df_time_naive = DTCvt.convert_utc_dt_df_to_naive(df)
            print('Selected {} ticks for {} btw {} and {} at price ${} from DB:'.format(
                len(df), contract.symbol, DTCvt.convert_utc_dt_to_syd_naive(dt_start_utc),
                DTCvt.convert_utc_dt_to_syd_naive(dt_end_utc),  price))
            print(df_time_naive[['time_naive', 'price', 'volume', 'exchange', 'symbol']].head())
        else:
            print('No ticks are selected from DB for {} btw {} and {} at price ${}!!!'.format(
                contract.symbol, dt_start_utc, dt_end_utc, price))
        return df
    except Exception as e:
        print(e)


def select_all_ticks_for_date_contr(db_connection, contract_id: int, dt_naive: dt.date):
    d_utc = DTCvt.convert_naive_dt_to_utc(dt_naive).date()
    dt_start_utc = dt.datetime.combine(d_utc, dt.time.min)
    dt_end_utc = dt.datetime.combine(d_utc, dt.time.max)
    query = "SELECT * FROM tick WHERE time BETWEEN %s AND %s AND contract_id = %s ORDER BY time"
    values = [dt_start_utc, dt_end_utc, contract_id]
    try:
        df = pd.read_sql_query(con=db_connection, sql=query, params=values)
        df.sort_values(by=['time'], inplace=True)
        print('Selected ticks btw {} and {} for contract_id {} from DB:'.format(dt_start_utc, dt_end_utc, contract_id))
        print(df)
        return df
    except Exception as e:
        print(e)


def select_distinct_tick_symbols_btw_dt(db_connection, dt_start: dt.datetime, dt_end: dt.datetime):
    query = "SELECT DISTINCT symbol FROM tick " \
            "WHERE time BETWEEN %s and %s " \
            "ORDER BY symbol"
    values = [dt_start, dt_end]
    try:
        df = pd.read_sql_query(con=db_connection, sql=query, params=values)
        sym_lst = []
        for i in range(len(df)):
            sym_lst.append(df.loc[i]['symbol'])
            # print('symbol: {}'.format(df.loc[i]['symbol']))
        print(sym_lst)
        return df
    except Exception as e:
        print(e)


def select_ticks_btw_dt(db_connection, dt_start: dt.datetime, dt_end: dt.datetime):
    query = "SELECT DISTINCT symbol, time, exchange, volume FROM tick " \
            "WHERE time BETWEEN %s and %s " \
            "ORDER BY symbol, time"
    values = [dt_start, dt_end]
    try:
        df = pd.read_sql_query(con=db_connection, sql=query, params=values)
        df_naive = DTCvt.convert_utc_dt_df_to_naive(df)
        print(df_naive)
        return df_naive
    except Exception as e:
        print(e)


def filter_tick_sym_not_start_w_phase_letters(db_connection, DF: pd.DataFrame):
    ticker_start_with_lst = StcVbl.ASX_open_phases[1][0]
    for i in range(len(DF)):
        sym = DF.loc[i]['symbol']
        if sym[0].lower() not in ticker_start_with_lst:
            exchange = DF.loc[i]['exchange']
            t = DF.loc[i]['time_naive']
            contr_df = select_common_contr_by_symbol(db_connection, sym)
            print('symbol: {}, stock_type: {}, exchange: {}, time: {}, volume: {}, name: {}'.format(
                contr_df.loc[0]['symbol'], contr_df.loc[0]['stock_type'], exchange, t,
                contr_df.loc[0]['volume'], contr_df.loc[0]['full_name']))


# ------------------------- Functions relates to processing XMLs ---------------------------


def get_mkt_cap(db_connection, contract: Contract):
    pass


def get_share_issued(db_connection, contract: Contract):
    # contract -> org_id ->
    query = 'SELECT * FROM share_issued'


def get_share_float(db_connection, contract: Contract):
    pass


def get_share_split(db_connection, contract: Contract):
    pass


def get_snapshot_xml(db_connection, contract: Contract):
    try:
        query = 'SELECT contract_id, symbol, snapshot_xml FROM fundamental_xmls WHERE contract_id=%s AND symbol=%s'
        values = [contract.conId, contract.symbol]
        df = pd.read_sql_query(query, db_connection, params=values)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_snapshot_xml(db_connection):
    try:
        query = 'SELECT contract_id, symbol, snapshot_xml FROM fundamental_xmls WHERE snapshot_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_fin_stmts_xml(db_connection):
    try:
        query = 'SELECT contract_id, symbol, fin_stmts_xml FROM fundamental_xmls WHERE fin_stmts_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_fin_stmts_xml_when_snapshot_null(db_connection):
    try:
        query = 'SELECT contract_id, symbol, fin_stmts_xml FROM fundamental_xmls ' \
                'WHERE snapshot_xml is null AND fin_stmts_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_fin_summary_xml(db_connection):
    try:
        query = 'SELECT contract_id, symbol, fin_summary_xml FROM fundamental_xmls WHERE fin_summary_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_ratios_xml(db_connection):
    try:
        query = 'SELECT contract_id, symbol, ratios_xml FROM fundamental_xmls WHERE ratios_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def get_all_research_xml(db_connection):
    try:
        query = 'SELECT contract_id, symbol, research_xml FROM fundamental_xmls WHERE research_xml IS NOT NULL'
        df = pd.read_sql_query(query, db_connection)
        print(df)
        return df
    except Exception as e:
        print(e)


def join_fundxmls_with_share_issued(db_connection):
    try:
        query = 'SELECT fundamental_xmls.contract_id, symbol, fundamental_xmls.org_id, date, number ' \
                'FROM fundamental_xmls ' \
                'INNER JOIN share_issued ' \
                'ON fundamental_xmls.org_id = share_issued.org_id'
        df = pd.read_sql_query(sql=query, con=db_connection, index_col='contract_id')
        print('\nGet number of shares issued for {} organizations in DB:'.format(len(df)))
        print(df[['org_id', 'date', 'number']].describe())
        return df
    except Exception as e:
        print(e)


# db_conn = DBWrt.connect_to_db()
# start = dt.datetime(2021, 3, 8, 9, 59, 45)
# end = dt.datetime(2021, 3, 8, 10, 1, 59, 999999)
# df_distinct = select_distinct_tick_symbols_btw_dt(db_conn, start, end)
# df = select_ticks_btw_dt(db_conn, start, end)
# filter_tick_sym_not_start_w_phase_letters(db_conn, df)
