# get_real_time_stock_data_from_google_finance_multi_threaded

The purpose of this program is to retrieve  Google Intraday Data for a list of symbols from the NSE stock exchange . Designed to run until 15:30 hrs Indian Standard Time(IST)

The Input file for this program is an Excel Spreadsheet. Name it as "stock_symbols". Enter the list of stocks in the  first column of the sheet for which you would like to retrieve the Google Intraday Data. 

This is a multi threaded program that uses the  concurrent.futures  for asynchronously executing callables

The following arguments are required for the program :  sheetname time_in_minutes

Write's the output in OHLCV format in a CSV file

When tested on a Intel I3-2367M processor for a list of 200 stocks:

    For sequential processing , the time taken was about 30 seconds.
  
    For asynchronous processing, the time taken was about 16 seconds.
