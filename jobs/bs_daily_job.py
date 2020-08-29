#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import libs.common as common
import sys
import time
import pandas as pd
import baostock as bs
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import datetime

"""
交易数据

http://baostock.com/baostock/index.php/Python_API%E6%96%87%E6%A1%A3#HelloWorld
从bs拉取每天全部股票的k数据
"""


def query_stock_k_data_plus(date):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)
    date = date.strftime("%Y-%m-%d")
    print(date)
    stock_rs = bs.query_all_stock(date)
    stock_df = stock_rs.get_data()
    print(stock_df.empty)
    if not (stock_df is None or stock_df.empty):
        data_df = pd.DataFrame()
        for code in stock_df["code"]:
            k_rs = bs.query_history_k_data_plus(code, "date,code,open,high,low,close,preclose,volume,"
                                                      "amount,adjustflag,turn,tradestatus,pctChg,peTTM,"
                                                      "pbMRQ,psTTM,pcfNcfTTM,isST", date, date)
            data_df = data_df.append(k_rs.get_data())
        data = data_df.drop_duplicates(subset={"date", "code"}, keep="last")
        common.insert_db(data, "bs_stock_k_data", False, "`date`,`code`")
        print(date + " done!")
    else:
        print(date + " no data .")
    bs.logout()


# main函数入口
if __name__ == '__main__':
    # 使用方法传递
    common.run_with_args(query_stock_k_data_plus)
