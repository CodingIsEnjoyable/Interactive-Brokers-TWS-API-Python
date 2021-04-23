import enum

import psycopg2
import psycopg2.extras
import datetime as dt
import pandas as pd

import DataStorage as DS
import DatetimeConverter as DTC
# import DatabaseQuery as DBQ
# import FileProcessor as FP
import StaticVariables as StcVbls

from io import StringIO

from ibapi.client import BarData
from ibapi.common import HistoricalTickLast
from ibapi.contract import ContractDetails, Contract
# from FileProcessor import readAsxStocksList


def connect_to_db():
    connect_query = "postgres://postgres:your_password@localhost:5432/your_database_name"
    connection = psycopg2.connect(connect_query)
    return connection


"""The Australian share market is broken up into 11 Sectors, 24 Industry Groups, 68 Industries and 157 Sub-Industries. 
The ASX has no companies in 9 Sub-Industries. On 16 September August 2016 the 11th Sector "Real Estate" was added.

11 Sectors
24 Industry Groups
68 Industry Sub Groups
157 Sub Industry Groups

Companies are categorised according to their primary business activity following the Global Industry Classification Standard (GICS). 
As most established markets use GICS, investors can accurately compare the performance of Australian sectors/industries to those of other countries.

Metals/Mining - these include some of the world’s largest resource companies (such as BHP Billiton and Rio Tinto) as well exploration companiesIndustrials – this sector covers a broad range of industries including transport, infrastructure, construction and engineering.
Materials - including building materials, chemicals, fertilisers and agricultural commodities.
Financials - covering banks, insurance companies, asset managers and a range of other financial services providers.
Consumer Discretionary - covers a broad spectrum of companies including media, gambling and general retailers.
Consumer Staples - includes companies the focus on production and distribution of food and beverages.
Utilities - this includes major electricity and gas retailers as well as smaller utility providers.
Energy - explorers and producers of energy resources including oil, gas and renewable energies.
Health Care - medical practice companies, pharmaceuticals, pathology companies, medical device companies and biotechnology.
Information Technology - includes software developers and IT consultancies. Companies in this sector tend to be small to medium-sized businesses.
Real Estate - companies and investment trusts solely focused on residential, commercial and industrial real estate.
Telecommunications Services - includes telecommunication carriers, distributors and resellers.

https://www.marketindex.com.au/asx-sectors
https://www.investors.asn.au/education/shares/understanding-shares/asx-sectors/
"""

"""
Important reference to parameterised queries read this:
https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
"""


# ======================== Contract Functions ==============================

def insert_into_contract(db_connection, contract_details: ContractDetails):
    try:
        dtls = contract_details
        contr = contract_details.contract
        query = 'INSERT INTO contract(contract_id, symbol, full_name, security_type, security_id, security_id_type, stock_type, currency, primary_exchange, exchange, valid_exchanges, sector, category, sub_category, time_zone, min_tick)' \
                + ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                + ' ON CONFLICT (contract_id) DO NOTHING;'
        values = [contr.conId, contr.symbol, dtls.longName, contr.secType, contr.secId, contr.secIdType, dtls.stockType,
                  contr.currency,
                  contr.primaryExchange, contr.exchange, dtls.validExchanges, dtls.industry, dtls.category,
                  dtls.subcategory, dtls.timeZoneId, dtls.minTick]
        db_connection.cursor().execute(query, values)
    except Exception as e:
        print(e)
    try:
        db_connection.commit()  # commit() must be called after execute
        print("Inserted Contract into DB. ticker: {}, contract_id: {}"
              .format(contr.symbol, contr.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_earliest_avbl_dt_in_contr(db_connection, earliest_dt: dt.datetime, contract: Contract):
    try:
        query = 'UPDATE contract SET earliest_avbl_dt = %s WHERE symbol = %s AND contract_id = %s'
        db_connection.cursor().execute(query, (earliest_dt, contract.symbol, contract.conId))
        db_connection.commit()
        print("Inserted Earliest Available Datetime into DB. Symbol: {}, contract_id: {}"
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_is_active_in_contr(db_connection, contract: Contract):
    # TODO: When 'No security is found error' returned from IB, is_active needs to be updated in contract table in DB
    try:
        query = 'UPDATE contract SET is_active = False WHERE contract_id = %s'
        values = [contract.conId]
        db_connection.cursor().execute(query, values)
        db_connection.commit()
        print('Updated Is_active to False in Contract in DB. Symbol: {}, Contract_id: {}'
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_snapshot_xml_in_contr(db_connection, contract: Contract, data: str):
    try:
        query = 'UPDATE contract SET profile_xml = %s WHERE symbol = %s AND contract_id = %s'
        db_connection.cursor().execute(query, (data, contract.symbol, contract.conId))
        db_connection.commit()
        print("Inserted Profile XML into DB. Symbol: {}, Contract_id: {}"
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_fin_stmts_in_contr(db_connection, contract: Contract, data: str):
    try:
        query = 'UPDATE contract SET fin_stmts_xml = %s WHERE symbol = %s AND contract_id = %s'
        db_connection.cursor().execute(query, (data, contract.symbol, contract.conId))
        db_connection.commit()
        print("Inserted Financial Stmt XML into DB. Symbol: {}, contract_id: {}"
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_fin_summary_in_contr(db_connection, contract: Contract, data: str):
    try:
        query = 'UPDATE contract SET fin_summary_xml = %s WHERE symbol = %s AND contract_id = %s'
        db_connection.cursor().execute(query, (data, contract.symbol, contract.conId))
        db_connection.commit()
        print("Inserted Financial Summary XML into DB. Symbol: {}, contract_id: {}"
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


# ============================== Bar Functions ==============================

def insert_into_bar(db_connection, time_stamp: dt.datetime, bar: BarData, time_span: str,
                    contract_details: ContractDetails):
    """ IMPORTANT! DO NOT USE THE STRING TIME VALUE IN BarData PARAMETER!
    MUST USE datetime format in time_stamp parameter."""
    query = 'INSERT INTO bar(time, contract_id, time_span, open, high, low, close, average, volume, trades, symbol) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    values = [time_stamp, contract_details.contract.conId, time_span, bar.open, bar.high, bar.low, bar.close,
              bar.average, bar.volume, bar.barCount, contract_details.contract.symbol]
    try:
        db_connection.cursor().execute(query, values)
        db_connection.commit()  # commit() must be called after execute
        # print("Inserted Bar into DB. ticker: {}, time: {}, time span: {}"
        #       .format(contract_details.contract.symbol, time_stamp, time_span))
    except Exception as e:
        db_connection.rollback()
        print(e)


def insert_into_bar1(db_connection, dt_utc: dt.datetime, bar: BarData, time_span: str, contr: Contract):
    """ IMPORTANT! DO NOT USE THE STRING TIME VALUE IN BarData PARAMETER!
    MUST USE datetime format in dt_utc parameter."""
    query = 'INSERT INTO bar(time, contract_id, symbol, time_span, open, high, low, close, average, volume, trades) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    values = [dt_utc, contr.conId, contr.symbol, time_span, bar.open, bar.high, bar.low, bar.close,
              bar.average, bar.volume, bar.barCount]
    try:
        db_connection.cursor().execute(query, values)
        db_connection.commit()  # commit() must be called after execute
        dt_syd_naive = DTC.convert_utc_dt_to_syd_naive(dt_utc)
        print("Inserted bar into DB. Symbol: {}, time: {}, bar size: {}, open: {}, high: {}, low: {}, "
              "close: {}, avg: {}, vol: {}, trades: {}".format(contr.symbol, dt_syd_naive, time_span,
                                                               bar.open, bar.high, bar.low, bar.close,
                                                               bar.average, bar.volume, bar.barCount))
    except Exception as e:
        db_connection.rollback()
        print(e)


def insert_into_auction(db_connection, auction_series: pd.Series, auction_type_str: str):
    query = 'INSERT INTO auction(auc_type, contract_id, symbol, time, price, volume, trades, money_vol, exchange) ' \
            'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    values = [auction_type_str, auction_series['contract_id'], auction_series['symbol'],
              auction_series['time'], auction_series['price'], auction_series['volume'],
              auction_series['trades'], auction_series['money_vol'], auction_series['exchange']]
    try:
        db_connection.cursor().execute(query, values)
        db_connection.commit()
        dt_naive = auction_series['time_naive']
        print('Inserted {}-Market Auction Data into DB. Symbol: {}, time: {}, price: {}, volume: {}, trade: {}, exchange: {}'.format(
            auction_type_str.upper(), auction_series['symbol'], dt_naive, auction_series['price'],
            auction_series['volume'], auction_series['trades'], auction_series['exchange']))
        a = 1
    except Exception as e:
        db_connection.rollback()
        print('Insert_into_auction Exception: {}!!!!!!'.format(e))


# ============================== Tick Functions ==============================

def insert_into_tick(db_connection, dt_naive: dt.datetime, tick: HistoricalTickLast, contract: Contract):
    try:
        query = 'INSERT INTO tick(time, contract_id, symbol, price, volume, exchange, unreported, past_limit, special_conditions) ' \
                'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        values = [dt_naive, contract.conId, contract.symbol, tick.price, tick.size, tick.exchange,
                  tick.tickAttribLast.unreported, tick.tickAttribLast.pastLimit, tick.specialConditions]
        db_connection.cursor().execute(query, values)
        db_connection.commit()  # commit() must be called after execute
        print("Inserted Tick into DB. Symbol: {}, time: {}, price: {}, volume: {}, exchange: {}"
              .format(contract.symbol, dt_naive, tick.price, tick.size, tick.exchange))
    except Exception as e:
        db_connection.rollback()
        print(e)


def insert_df_into_tick(db_connection, df: pd.DataFrame):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = db_connection.cursor()
    try:
        cursor.copy_from(buffer, table='tick', sep=",", columns=('contract_id', 'symbol', 'time', 'price', 'volume',
                                                                 'exchange', 'past_limit', 'unreported', 'special_conditions'))
        db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db_connection.rollback()
        cursor.close()
        return 1
    print('Inserted tick data df into DB! Length of df is {}.'.format(len(df)))
    cursor.close()


def insert_df_into_tick1(db_connection, df: pd.DataFrame):
    cursor = db_connection.cursor()
    try:
        for index, row in df.iterrows():
            query = 'INSERT INTO tick(contract_id, symbol, time, price, volume, exchange, past_limit, unreported, special_conditions) ' \
                    'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            values = [row['contract_id'], row['symbol'], row['time'], row['price'], row['volume'], row['exchange'],
                      row['past_limit'], row['unreported'], row['special_conditions']]
            cursor.execute(query, values)
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        db_connection.rollback()
        cursor.close()
        return 1
    print('Inserted tick data df into DB! Length of df is {}.'.format(len(df)))
    cursor.close()


# ============================== Depth Functions ==============================

def insert_into_depth(db_connection, dt_utc: dt.datetime, contract: Contract, position: int,
                      bid_ask_side: int, price: float, volume: int, operation: int,
                      mkt_maker: str, is_smt_depth: bool):
    try:
        query = 'INSERT INTO depth' \
                '(time, contract_id, symbol, position, bid_ask_side, price, volume, operation, mkt_maker, is_smart_depth) ' \
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        values = [dt_utc, contract.conId, contract.symbol, position, bid_ask_side, price, volume, operation, mkt_maker,
                  is_smt_depth]
        db_connection.cursor().execute(query, values)
    except Exception as e:
        print(e)
    try:
        db_connection.commit()  # commit() must be called after execute
        dt_syd_naive = DTC.convert_utc_dt_to_syd_naive(dt_utc)
        # print("Inserted Depth into DB. Symbol: {}, Time: {}, Contract_id: {}"
        #       .format(contract.symbol, dt_syd_naive, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


# ============================== Organization Functions ==============================

def insert_into_org(db_connection, org: DS.Organization):
    try:
        query1 = 'INSERT INTO org(org_id, name) VALUES(%s, %s)'
        values1 = [org.org_id, org.name]
        db_connection.cursor().execute(query1, values1)
        db_connection.commit()  # commit() must be called after execute
        print("Inserted Org into DB. Name: {}, Org_Id: {}"
              .format(org.name, org.org_id))
    except Exception as e:
        db_connection.rollback()
        print(e)
    try:
        query2 = 'INSERT INTO share_issued(org_id, date, number) VALUES(%s, %s, %s)'
        values2 = [org.org_id, org.share_issued.date, org.share_issued.number]
        db_connection.cursor().execute(query2, values2)
        db_connection.commit()  # commit() must be called after execute
        print("Inserted Share_Issued into DB. Name: {}, Org_Id: {}, Number: {}"
              .format(org.name, org.org_id, org.share_issued.number))
    except Exception as e:
        db_connection.rollback()
        print(e)
    try:
        query3 = 'INSERT INTO share_float(org_id, date, number) VALUES(%s, %s, %s)'
        values3 = [org.org_id, org.share_float.date, org.share_float.number]
        db_connection.cursor().execute(query3, values3)
        db_connection.commit()  # commit() must be called after execute
        print("Inserted Share_Float into DB. Name: {}, Org_Id: {}, Number: {}"
              .format(org.name, org.org_id, org.share_float.number))
    except Exception as e:
        db_connection.rollback()
        print(e)
    try:
        if org.share_split:
            query4 = 'INSERT INTO share_split(org_id, date, number) VALUES(%s, %s, %s)'
            values4 = [org.org_id, org.share_split.date, org.share_split.number]
            db_connection.cursor().execute(query4, values4)
            db_connection.commit()  # commit() must be called after execute
            print("Inserted Share_Split into DB. Name: {}, Org_Id: {}, Number: {}"
                  .format(org.name, org.org_id, org.share_split.number))
        else:
            print("org.share_split is None! Name: {}, Org_Id: {}, Number: {}".format(
                org.name, org.org_id, org.share_split.number))
    except Exception as e:
        db_connection.rollback()
        print(e)


def update_orgId_in_fundamental(db_connection, org: DS.Organization, contract_symbol: str, contract_id: int):
    query = 'UPDATE fundamental_xmls SET org_id=%s WHERE contract_id=%s AND symbol=%s'
    values = [org.org_id, contract_id, contract_symbol]
    try:
        db_connection.cursor().execute(query, values)
        db_connection.commit()
        print("Updated OrgId in DB. Symbol: {}, ConId: {}, OrgId: {}".format(
            contract_symbol, contract_id, org.org_id))
    except Exception as e:
        db_connection.rollback()
        print(e)


# ============================== XML Functions ==============================

def insert_into_snapshot_xml(db_connection, contract: Contract, data: str):
    try:
        query = 'INSERT INTO xml_snapshot(contract_id, symbol, date, xml_snapshot)' \
                'VALUES ()'
        db_connection.cursor().execute(query, (data, contract.symbol, contract.conId))
        db_connection.commit()
        print("Inserted Profile XML into DB. Symbol: {}, Contract_id: {}"
              .format(contract.symbol, contract.conId))
    except Exception as e:
        db_connection.rollback()
        print(e)


# ================================ Testing Code ==============================

"""
def test_snapshot_xml():
    conn = connect_to_db()
    xml_df = DBQ.get_all_snapshot_xml(conn)
    for key, row in xml_df.iterrows():
        org = FP.parse_snapshot_xml(row['snapshot_xml'])
        # update_orgId_in_fundamental(conn, org, row['symbol'], row['contract_id'])
        insert_into_org(conn, org)


def test_fin_stmts_xml():
    conn = connect_to_db()
    # fin_df = DBQ.get_all_fin_stmts_xml(conn)
    f_df = DBQ.get_fin_stmts_xml_when_snapshot_null(conn)
    for key, row in f_df.iterrows():
        # print(row['fin_stmts_xml'])
        f_lst = FP.parse_fin_stmts_xml(row['fin_stmts_xml'])
        update_orgId_in_fundamental(conn, f_lst[0], row['symbol'], row['contract_id'])
"""


def test1():
    # tz_str_syd = 'Australia/Sydney'
    # tz_syd = pytz.timezone(tz_str_syd)
    # fmt_str_tz = '%Y-%m-%d %H:%M:%S%z'
    conn = connect_to_db()
    # exists = check_contract_exists_in_bar(conn, 'CBA', 4036818)
    # if exists:
    #     # first = 2020-06-26 00:00:00+10:00  last = 2020-12-24 00:00:00+11:00
    #     first_dt_utc, last_dt_utc = get_bar_time_last_first(conn, 'CBA', 4036818, vbl.bar_size_str.one_day.value)
    #     dt_now_utc = dt.datetime.now(tz_syd).astimezone()
    #     end_dt_str = dt.datetime.strftime(dt_now_utc, '%Y%m%d %H:%M:%S')
    #     diff = dt_now_utc - last_dt_utc
    #     duration = diff.days + 1
    #     duration_str = str(duration) + ' D'
    #     a = 1


