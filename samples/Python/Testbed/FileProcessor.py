import pandas as pd
import datetime as dt
import xml.etree.ElementTree as ETree

import DataStorage as DStrg
import DatabaseQuery as DBQry
import DatabaseWriter as DBWtr

from ibapi.contract import Contract

csv_file_path = 'C:/Users/Rosie/Documents/Python Generated Files/'


# ================================ Functions to Read Files =================================

def readAsxStocksList():
    data = pd.read_excel(r'C:\Users\Rosie\Documents\Trading\ASX_All_Stock_List_20201224.xlsx', engine='openpyxl')
    df = pd.DataFrame(data, columns=['Rank', 'Code', 'Company', 'Price', 'Mkt Cap', '1 Year'])
    # print(df)
    tickers = df['Code']
    return tickers


def read_excel_multi_sheets(excel_file_name: str):
    data = pd.read_excel(excel_file_name, engine='openpyxl')
    df = pd.DataFrame(data, columns=['Rank', 'Code', 'Company', 'Price', 'Mkt Cap', '1 Year'])
    # print(df)
    tickers = df['Code']
    return tickers


# ================================ Functions to Write Files =================================


def write_multiple_df_to_one_excel(excel_file_name: str, daily_bars_dict: {}, intra_day_bars_dict: {}):
    print("Writing all daily bars and intra day bars in dictionaries to excel {}:".format(excel_file_name))
    for key in daily_bars_dict.keys():
        with pd.ExcelWriter(excel_file_name, mode='a') as writer:
            daily_bars_df = daily_bars_dict[key]
            intra_day_bars_df = intra_day_bars_dict[key]
            daily_bars_df.drop('time', axis=1, inplace=True)
            intra_day_bars_df.drop('time', axis=1, inplace=True)
            sym = daily_bars_df.loc[0, 'symbol']
            print('Appending {} daily bars to excel sheet \'daily_bars_{}\''.format(sym, sym))
            daily_bars_df.to_excel(writer, sheet_name='daily_bars_{}'.format(sym))
            print('Appending {} intra day bars to excel sheet \'intra_day_bars_{}\''.format(sym, sym))
            intra_day_bars_df.to_excel(writer, sheet_name='intra_day_bars_{}'.format(sym))


def write_df_to_csv(df: pd.DataFrame, file_name: str):
    print('Writing dataframe to a csv file {}{}'.format(csv_file_path, file_name))
    df.to_csv(csv_file_path + file_name, index=False, float_format='%.4f')


def save_all_asx_contracts_to_csv(db_connection):
    df = DBQry.select_all_contracts_for_primEX(db_connection, 'ASX')
    write_df_to_csv(df, 'ASX_all_contracts_20210311.csv')


def save_all_sub_categories_to_csv(db_connection):
    df = DBQry.select_all_sub_categories_fr_contracts(db_connection)
    write_df_to_csv(df, 'ASX_all_sub_categories.csv')


def save_all_REITS_to_csv(db_connection):
    df = DBQry.select_all_reits_contracts(db_connection)
    write_df_to_csv(df, 'ASX_all_reits_contracts.csv')


def save_all_gold_mining_contracts_to_csv(db_connection):
    df = DBQry.select_all_gold_mining_contracts(db_connection)
    write_df_to_csv(df, 'ASX_all_gold_mining_contracts.csv')


def test_save_to_csv():
    conn = DBWtr.connect_to_db()
    save_all_asx_contracts_to_csv(conn)
    save_all_sub_categories_to_csv(conn)
    save_all_REITS_to_csv(conn)
    save_all_gold_mining_contracts_to_csv(conn)


# test_save_to_csv()


# ================================ Functions to Handle XML =================================


def open_xml():
    f = open("ASX_SYD_Fundamental_Snapshot.xml", "r")
    xml_str = f.read()
    f.close()
    return xml_str


def writeXml(self, contr: Contract, data: str):
    f = open("{}_{}_Fundamental_Snapshot.xml".format(contr.primaryExchange, contr.symbol), "w")
    f.write(data)
    f.close()


def parse_snapshot_xml(xml_str: str):
    tree = ETree.fromstring(xml_str)
    for child in tree:
        if child.tag == 'CoIDs':
            for id_elm in child:
                if id_elm.get('Type') == 'CompanyName':
                    co_name = id_elm.text
                elif id_elm.get('Type') == 'OrganizationPermID':
                    co_id = float(id_elm.text)
            org = DStrg.Organization(co_id, co_name)
        if child.tag == 'Issues':  # contains split info
            for iss_elm in child:
                # <Issue ID="1" Type="C" Desc="Common Stock" Order="1">
                # <Issue ID="2" Type="P" Desc="Preferred Stock" Order="1">
                if iss_elm.get('Type') == 'C':
                    for comm_stk_elm in iss_elm:
                        if comm_stk_elm.tag == 'MostRecentSplit':
                            last_split_date = dt.datetime.strptime(comm_stk_elm.get('Date'), '%Y-%m-%d').date()
                            last_split = float(comm_stk_elm.text)
                            org.share_split = DStrg.ShareSplit(last_split_date, last_split)
        if child.tag == 'CoGeneralInfo':  # contains number of shares
            for gi_elm in child:
                # <SharesOut Date="2020-06-30" TotalFloat="939774578.0">942286900.0</SharesOut>
                if gi_elm.tag == 'SharesOut':
                    num_float = float(gi_elm.get('TotalFloat'))
                    num_shares = float(gi_elm.text)
                    num_shares_date = dt.datetime.strptime(gi_elm.get('Date'), '%Y-%m-%d').date()
                    org.share_issued = DStrg.ShareIssued(num_shares_date, num_shares)
                    org.share_float = DStrg.ShareFloat(num_shares_date, num_float)
        if child.tag == 'Ratios':  # contains mkt cap
            for r_elm in child:
                if r_elm.get('ID') == 'Income Statement':
                    for pv_elm in r_elm:
                        if pv_elm.get('FieldName') == 'MKTCAP':
                            mkt_cap = float(pv_elm.text) * 1000000
                            org.mkt_cap = mkt_cap
    return org


def parse_fin_stmts_xml(xml_str: str):
    tree = ETree.fromstring(xml_str)

    org = None
    name_map = {}
    inc_st_dict = {}
    bs_st_dict = {}
    cf_st_dict = {}
    interim_inc_st_dict = {}
    interim_bs_st_dict = {}
    interim_cf_st_dict = {}

    for child in tree:
        if child.tag == 'CoIDs':
            for id_elm in child:
                co_name = ''
                co_id = 0
                if id_elm.get('Type') == 'CompanyName':
                    co_name = id_elm.text
                # <CoID Type="RepNo">A4EC6</CoID>
                # <CoID Type="CompanyName">SPDR S&amp;P/ASX 50</CoID>
                elif id_elm.get('Type') == 'OrganizationPermID':
                    co_id = float(id_elm.text)
            if co_id == '':
                print('OrganizationPermID does NOT exist in Financial Stmts Xml!')
            org = DStrg.Organization(co_id, co_name)  # Not all fin_stmts_xml has CompanyId !!!
        elif child.tag == 'FinancialStatements':
            for fs_elm in child:
                if fs_elm.tag == 'COAMap':
                    for coa_elm in fs_elm:
                        name_map[coa_elm.get('coaItem')] = coa_elm.text
                elif fs_elm.tag == 'AnnualPeriods':
                    for ap_elm in fs_elm:
                        # <FiscalPeriod Type="Annual" EndDate="2020-06-30" FiscalYear="2020">
                        ap_date = ap_elm.get('EndDate')
                        inc_st_dict[ap_date] = {}
                        bs_st_dict[ap_date] = {}
                        cf_st_dict[ap_date] = {}

                        for fiscal in ap_elm:
                            if fiscal.get('Type') == 'INC':  # Income Stmt
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        inc_st_dict[ap_date][is_elm.get('coaCode')] = is_elm.text

                            elif fiscal.get('Type') == 'BAL':  # Balance Sheet
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        bs_st_dict[ap_date][is_elm.get('coaCode')] = is_elm.text

                            elif fiscal.get('Type') == 'CAS':  # Cashflow Stmt
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        cf_st_dict[ap_date][is_elm.get('coaCode')] = is_elm.text

                elif fs_elm.tag == 'InterimPeriods':
                    for ip_elm in fs_elm:
                        # <FiscalPeriod Type="Interim" EndDate="2020-06-30" FiscalYear="2020" FiscalPeriodNumber="4">
                        ip_date = ip_elm.get('EndDate')
                        period_no = ip_elm.get('FiscalPeriodNumber')
                        period_combine = ip_date + '_period' + period_no
                        interim_inc_st_dict[period_combine] = {}
                        interim_bs_st_dict[period_combine] = {}
                        interim_cf_st_dict[period_combine] = {}

                        for fiscal in ip_elm:
                            if fiscal.get('Type') == 'INC':  # Income Stmt
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        interim_inc_st_dict[period_combine][is_elm.get('coaCode')] = is_elm.text

                            elif fiscal.get('Type') == 'BAL':  # Balance Sheet
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        interim_bs_st_dict[period_combine][is_elm.get('coaCode')] = is_elm.text

                            elif fiscal.get('Type') == 'CAS':  # Cashflow Stmt
                                for is_elm in fiscal:
                                    if is_elm.tag == 'lineItem':
                                        interim_cf_st_dict[period_combine][is_elm.get('coaCode')] = is_elm.text

    inc_df = pd.DataFrame(inc_st_dict)
    bs_df = pd.DataFrame(bs_st_dict)
    cf_df = pd.DataFrame(cf_st_dict)
    int_inc_df = pd.DataFrame(interim_inc_st_dict)
    int_bs_df = pd.DataFrame(interim_bs_st_dict)
    int_cf_df = pd.DataFrame(interim_cf_st_dict)

    inc_df.rename(name_map, inplace=True)
    bs_df.rename(name_map, inplace=True)
    cf_df.rename(name_map, inplace=True)
    int_inc_df.rename(name_map, inplace=True)
    int_bs_df.rename(name_map, inplace=True)
    int_cf_df.rename(name_map, inplace=True)

    print(inc_df)
    print(bs_df)
    print(cf_df)
    print(int_inc_df)
    print(int_bs_df)
    print(int_cf_df)
    return [org, inc_df, bs_df, cf_df]
