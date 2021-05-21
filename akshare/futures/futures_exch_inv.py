# -*- coding:utf-8 -*-
# /usr/bin/env python
"""
Date: 2021/3/29 13:18
Desc: 大连商品交易所, 上海期货交易所, 郑州商品交易所采集每日注册仓单数据
"""
import datetime
import re
import warnings
from typing import List

import pandas as pd
import requests

from akshare.futures import cons
from akshare.futures.requests_fun import requests_link, pandas_read_html_link
from akshare.futures.symbol_var import chinese_to_english, find_chinese

calendar = cons.get_calendar()

def get_shfe_inv_1(date: str = None, vars_list: List = cons.contract_symbols):
    """
    抓取上海期货交易所注册仓单数据, 适用20081006至20140518(包括) 20100126、20101029日期交易所格式混乱，直接回复脚本中DataFrame, 20100416、20130821日期交易所数据丢失
    :param date: 开始日期 format：YYYY-MM-DD 或 YYYYMMDD 或 datetime.date对象 为空时为当天
    :param vars_list: 合约品种如RB、AL等列表 为空时为所有商品
    :return: pd.DataFrame
    展期收益率数据(DataFrame):
                    var             商品品种                     string
                    receipt         仓单数                       int
                    date            日期                         string YYYYMMDD           
    """
    date = cons.convert_date(date).strftime('%Y%m%d') if date is not None else datetime.date.today()
    if date not in calendar:
        warnings.warn(f"{date.strftime('%Y%m%d')}非交易日")
        return None
    if date in ['20100416', '20130821']:
        return warnings.warn('20100416、20130821日期交易所数据丢失')
    else:
        var_list = ['天然橡胶', '沥青仓库', '沥青厂库', '热轧卷板', '燃料油', '白银', '线材', '螺纹钢', '铅', '铜', '铝', '锌', '黄金', '锡', '镍']
        url = cons.SHFE_INV_URL_1 % date
        data = pandas_read_html_link(url)[0]
        indexes = []
        last_indexes = []
        data_list = data[0].tolist()
        for x in data.index:
            prod_key = str(data_list[x]).split('：')[-1]
            if prod_key in var_list:                    
                indexes.append(x)
                continue
            if ('总 计' in str(data_list[x])):
                last_indexes.append(x)
            elif ('注:' in str(data_list[x])):
                if x-1 not in last_indexes:
                    last_indexes.append(x-1) 
        records = pd.DataFrame()
        for i in list(range(len(indexes))):
            data_cut = data.loc[indexes[i]:last_indexes[i], :]
            data_dict = dict()
            prod_key = data_cut[0].tolist()[0].split('：')[-1]
            prod_key = prod_key = prod_key.replace("(", "").replace(")", "")
            data_dict['var'] = chinese_to_english(prod_key)
            data_dict['var_label'] = prod_key
            if prod_key in ['沥青仓库', '燃料油', '天然橡胶', '铅', '铜', '铝', '锌', '锡', '镍']:
                data_dict['spot_inventory'] = int(data_cut[4].tolist()[-1])
                data_dict['warrant_inventory'] = int(data_cut[5].tolist()[-1])
                data_dict['warehouse_stocks'] = int(data_cut[9].tolist()[-1])
            elif prod_key in ['沥青厂库', '热轧卷板', '白银', '线材', '螺纹钢']: 
                data_dict['spot_inventory'] = 0
                data_dict['warrant_inventory'] = int(data_cut[3].tolist()[-1])
                data_dict['warehouse_stocks'] = int(data_cut[6].tolist()[-1]) 
            elif prod_key in ['黄金']:                        
                data_dict['spot_inventory'] = 0
                data_dict['warrant_inventory'] = int(data_cut[1].tolist()[-1])
                data_dict['warehouse_stocks'] = 0
            data_dict['date'] = date
            records = records.append(pd.DataFrame(data_dict, index=[1]))
    # if len(records.index) != 0:
    #     records.index = records['var']
    #     vars_in_market = [i for i in vars_list if i in records.index]
    #     records = records.loc[vars_in_market, :]
    return records.reset_index(drop=True)


def get_shfe_inv_2(date: str = None, vars_list: List = cons.contract_symbols):
    """
        抓取上海商品交易所注册仓单数据
        适用20140519(包括)至今
        Parameters
        ------
            date: 开始日期 format：YYYY-MM-DD 或 YYYYMMDD 或 datetime.date对象 为空时为当天
            vars_list: 合约品种如RB、AL等列表 为空时为所有商品
        Return
        -------
            DataFrame:
                展期收益率数据(DataFrame):
                    var             商品品种                     string
                    receipt         仓单数                       int
                    date            日期                         string YYYYMMDD
    """
    date = cons.convert_date(date).strftime('%Y%m%d') if date is not None else datetime.date.today()
    if date not in calendar:
        warnings.warn('%s非交易日' % date.strftime('%Y%m%d'))
        return None
    url = cons.SHFE_INV_URL_2 % date
    r = requests_link(url, encoding='utf-8')
    try:
        context = r.json()
    except:
        return pd.DataFrame()
    data = pd.DataFrame(context['o_cursor'])    
    if len(data.columns) < 1:
        return pd.DataFrame()
    records = pd.DataFrame()
    for var in set(data['VARNAME'].tolist()):
        data_cut = data[data['VARNAME'] == var]
        prod_key = var.split('$$')[0]
        prod_key = prod_key.replace("(", "").replace(")", "")
        if data_cut['SPOTWGHTS'].tolist()[-1] == '':
            spot_wrt = 0
        else:
            spot_wrt = int(data_cut['SPOTWGHTS'].tolist()[-1])
        if data_cut['WRTWGHTS'].tolist()[-1] == '':
            wrt_wrt = 0
        else:
            wrt_wrt = int(data_cut['WRTWGHTS'].tolist()[-1])
        if data_cut['WHSTOCKS'].tolist()[-1] == '':
            wh_stock = 0
        else:
            wh_stock = int(data_cut['WHSTOCKS'].tolist()[-1])                            
        data_dict = {'var': chinese_to_english(prod_key), 'var_label': prod_key, \
                     # chinese_to_english(re.sub(r"\W|[a-zA-Z]", "", var)),
                     'spot_inventory': spot_wrt,
                     'warrant_inventory': wrt_wrt,
                     'warehouse_stocks': wh_stock,
                     'date': date}
        records = records.append(pd.DataFrame(data_dict, index=[0]))
    # if len(records.index) != 0:
    #     records.index = records['var']
    #     vars_in_market = [i for i in vars_list if i in records.index]
    #     records = records.loc[vars_in_market, :]
    return records.reset_index(drop=True)

def get_shfe_inv(start_day: str = None, end_day: str = None, vars_list: List = cons.contract_symbols):
    """
    大宗商品注册仓单数量
    :param start_day: 开始日期 format：YYYY-MM-DD 或 YYYYMMDD 或 datetime.date对象 为空时为当天
    :type start_day: str
    :param end_day: 结束数据 format：YYYY-MM-DD 或 YYYYMMDD 或 datetime.date对象 为空时为当天
    :type end_day: str
    :param vars_list: 合约品种如RB、AL等列表 为空时为所有商品
    :type vars_list: str
    :return: 展期收益率数据
    :rtype: pandas.DataFrame
    """
    start_day = cons.convert_date(start_day) if start_day is not None else datetime.date.today()
    end_day = cons.convert_date(end_day) if end_day is not None else cons.convert_date(
        cons.get_latest_data_date(datetime.datetime.now()))
    records = pd.DataFrame()
    while start_day <= end_day:
        if start_day.strftime('%Y%m%d') not in calendar:
            warnings.warn(f"{start_day.strftime('%Y%m%d')}非交易日")
        else:
            print(start_day)            
            if datetime.date(2008, 10, 6) <= start_day <= datetime.date(2014, 5, 16):
                f = get_shfe_inv_1
            elif start_day > datetime.date(2014, 5, 16):
                f = get_shfe_inv_2
            else:
                f = None
                print('20081006起，shfe每交易日更新仓单数据')            
            get_vars = [var for var in vars_list if var in cons.market_exchange_symbols['shfe']]
            if get_vars != []:
                if f is not None:
                    records = records.append(f(start_day, get_vars))
        start_day += datetime.timedelta(days=1)
    records.reset_index(drop=True, inplace=True)
    return records
