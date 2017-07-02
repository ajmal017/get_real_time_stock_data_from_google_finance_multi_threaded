# -*- coding: utf-8 -*-
"""
Created on Sun Jul  2 22:19:12 2017

@author: pmullapudy
"""

from datetime import datetime, time
import pandas as pd
import requests
import time as ti
import os, glob, shutil
import argparse
#from multiprocessing import freeze_support
from concurrent import futures
#
# Global Variables.
close_time = time(15,30)
first_run = True # in the first run retrieve the data irrespective of the time
# Functions
def get_arguements():
    parser = argparse.ArgumentParser()
    parser.add_argument("sheetname", help="enter the sheetname: Sheet1 / Sheet2 / Sheet3")
    parser.add_argument("time_in_minutes", help="specify time interval in minutes for getting intraday data",
                        type=int)
    #
    args = parser.parse_args()
    sheet_name = args.sheetname
    interval_minutes = args.time_in_minutes
    print("input Entries are:", (sheet_name, interval_minutes))
    #
    return (sheet_name, interval_minutes)
#
def check_directory():
    full_path = os.path.realpath(__file__)
    current_directory = os.path.dirname(full_path)
    src_directory = current_directory + "\\temp_google_data\\"
    dest_directory = current_directory + "\\RTD from Google\\"
    src_search_criteria = src_directory + "*.csv"
    dest_search_criteria = dest_directory + "*.csv"
    src_files_with_csv_ext = glob.glob(src_search_criteria)
    dest_files_with_csv_ext = glob.glob(dest_search_criteria)
    #If directory does Not exist, create it.If it already exists, identify all the csv file in the directory and delete them.
    if not os.path.exists(src_directory):
        os.makedirs(src_directory)
    else:
        for ind_file in src_files_with_csv_ext:
            os.remove(ind_file)
    #
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)
    else:
        for ind_file in dest_files_with_csv_ext:
            os.remove(ind_file)
#
def get_shortlisted_symbols(sheet_name):

    input_file = "stock_symbols.xlsx"
    df_trend = pd.read_excel(input_file, sheet_name, usecols=[0], index_col=None)
    df_trend = df_trend.sort_values('Symbol')
    df_trend = df_trend.reset_index(drop=True) #This resets the index to the default integer index
    print ("# of shortlisted symbols is: ", len(df_trend))
    #convert the pandas DF to list
    symbols_list = df_trend.Symbol.tolist()
    return symbols_list
#
def get_urls(symbols_list):
    urls_list = list()
    exchange = 'NSE'
    # when 1 is added to interval_seconds, format of unixtimeatamp is appended to each element on google finance page. Check out these two url's
    #https://www.google.com/finance/getprices?i=300&p=5d&f=d,o,h,l,c,v&df=cpct&x=NSE&q=TCS
    #https://www.google.com/finance/getprices?i=301&p=5d&f=d,o,h,l,c,v&df=cpct&x=NSE&q=TCS
    interval_seconds_increment = (interval_minutes*60) + 1
    #
    if interval_minutes in range(0,4):
        num_days = 3
    elif interval_minutes in range(4,6):
        num_days = 5
    elif interval_minutes in range(6,16):
        num_days = 10
    elif interval_minutes in range(16,31):
        num_days = 18
    else:
        num_days = 36
    #
    for symbol in symbols_list:
        url_string = ('http://www.google.com/finance/getprices?i='
                    + str(interval_seconds_increment) + '&p=' + str(num_days)
                    + 'd&f=d,o,h,l,c,v&df=cpct&x=' + exchange.upper()
                    + '&q=' + symbol.upper())
        urls_list.append(url_string)
    return urls_list
#
def load_url(url_string):
    r = requests.get(url_string, timeout=30) #change timeout to 30 seconds
    return r.text
#
def re_load_url(url_string):
    r = requests.get(url_string, timeout=30) #change timeout to 30 seconds
    stock_str = str(r.text)
    stock_list = stock_str.split('\n')
    if stock_list[len(stock_list)-1] == "": # delete the last blank list
        del stock_list[-1]
    return stock_list
#
def process_and_write(url, r_text):
        pos = url.rfind('&q=') + 3
        symbol = url[pos:]
        #
        stock_str = str(r_text)
        stock_list = stock_str.split('\n')
        if stock_list[len(stock_list)-1] == "": # delete the last blank list
            del stock_list[-1]
        if len(stock_list) < 7: # In case the stock list has no OHLC data
            # Retry and retrieve the URL once again
            stock_list = re_load_url(url)
            if len(stock_list) < 7:
#                print ("stock list has no OHLC data for: ", url)
#                print ('len(stock_list) is:', len(stock_list))
                return
        stock_list = stock_list[7:]
        stock_list = [line.split(',') for line in stock_list]
        # In case google flags a  violation of terms  & conditions, it can be seen in this print statement
        # print ('symbol is', symbol)
        # print ('stock_list is:', stock_list)
        #
        df_stock = pd.DataFrame(stock_list, columns=['Date_Time','Close','High','Low','Open','Volume'])
        # Convert UNIX format to Datetime format
        df_stock['Date_Time'] = df_stock['Date_Time'].apply(lambda x: datetime.fromtimestamp(int(x[1:])))
        #
        df_stock['Open'] = df_stock['Open'].astype(float).apply(lambda x: round(x,2))
        df_stock['High'] = df_stock['High'].astype(float).apply(lambda x: round(x,2))
        df_stock['Low'] = df_stock['Low'].astype(float).apply(lambda x: round(x,2))
        df_stock['Close'] = df_stock['Close'].astype(float).apply(lambda x: round(x,2))
        #
        df_stock['Volume'] = df_stock['Volume'].astype(int)

        # make datetime as the index and drop  the column
        df_stock = df_stock.set_index(['Date_Time'], drop=True)

        # Reorder to OHLCV format
        df_stock = df_stock[['Open', 'High', 'Low', 'Close', 'Volume']]
        # write to csv
        full_path = os.path.realpath(__file__)
        current_directory = os.path.dirname(full_path)
        src_directory = current_directory + "\\temp_google_data\\"
        dest_directory = current_directory + "\\RTD from Google\\"
        # open file, write to a temporary directory and then close
        src_file = os.path.join(src_directory, (symbol + ".csv"))
        out_file = open(src_file , 'w')
        df_stock.to_csv(out_file)
        out_file.close()
        #The copy is necessiated so that any other program that is reading the csv files from the destination directory (RTD from Google) continually, will have modified date after the entire writing is done (and NOT while the writing is being done).
        dest_file = os.path.join(dest_directory, (symbol + ".csv"))
        shutil.copyfile(src_file, dest_file)
#
# Main Program Begins Here
if __name__ == '__main__':
    #
#    freeze_support()
    check_directory()
    (sheet_name, interval_minutes) = get_arguements()
    symbols_list = get_shortlisted_symbols(sheet_name)
    #print (symbols_list)
    urls_list = get_urls(symbols_list)
#    print ("length of url list", len(urls_list))
    while ((datetime.now().time() <= close_time)) | first_run:
        if first_run or ((datetime.now().minute % interval_minutes == 0) and (datetime.now().second == 0)) :
#            print ('start time of for loop is:', ti.strftime('%H:%M:%S'))
            loop_start_time = ti.time()
            with futures.ThreadPoolExecutor() as executor:
            #with futures.ProcessPoolExecutor() as executor:
                future_to_url = dict((executor.submit(load_url, url), url) for url in urls_list)
#                print ("future_to_url is:", future_to_url)
                for future in futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    if future.exception() is not None:
                        print('%r generated an exception: %s' % (url, future.exception()))
                    else:
                        process_and_write(url, future.result())
                        #print('%r page is %d bytes' % (url, len(future.result())))
                        #print (future.result())
#
            print("------- %s seconds for one full cycle of data retreival & write-------" % (ti.time() - loop_start_time)) # This is to test the performance of the for loop
            if first_run:
                first_run = False
            else:
                sleep_time = (interval_minutes*60) - (ti.time() - loop_start_time) - 60 # incase the first run is at 09:15:28
                if sleep_time > 0:
                    pass
                else:
                    sleep_time = 1
                ti.sleep(sleep_time)
    print ("***** End of Program *****")
#