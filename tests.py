# from datetime import datetime
# file = open("records.txt", "a")
# file.write("\n" + str(datetime.now().date()) + ":")
# file.write("\nError " + str(datetime.now().replace(microsecond=0)))
#
#
#
# def ban(num, num2):
#     file.write("\nbruh")
#     return num * num2
#
#
# ban(8,6)
#
#
# file.close()
#
# import pandas as pd
# # df = pd.read_csv('S&P500.csv')
# # print(df['ticker'][df['ticker'] == 'MJH'].sum())
# tickerList = []
# df = pd.read_csv('S&P500.csv')
# mainTickerList = [df['ticker'], df['newsDateLength']]
# #oldDf = oldDf.sort_values(by=['date', 'time'], ascending=[False, False])
#
# for i in range(len(mainTickerList[0])):
#     ticker = mainTickerList[0][i]
#     length = mainTickerList[1][i]
#     print(ticker + ": " + str(length))
#     if i > 20:
#         break


#
# file = open("records.txt", "a")
# file.write("\n" + str(datetime.now().date()) + " ("  + str(datetime.now().time().replace(microsecond=0)) + "):")
#
#
# from google.cloud import bigquery
#
# def databaseRead(client):
#
#     project = "the-utility-300815"
#     dataset_id = "stock_news"
#
#     dataset_ref = bigquery.DatasetReference(project, dataset_id)
#     table_ref = dataset_ref.table("SP500")
#     table = client.get_table(table_ref)
#     # ^ just puts it together
#
#     # returns entire database
#     # return client.list_rows(table).to_DataFrame()
#
#     # saves data by not retrieving any unneccesary columns (1/300 cost)
#     return client.query("SELECT date, time, ticker FROM `the-utility-300815.stock_news.SP500`").to_DataFrame()
#
# import pandas as pd
# client = bigquery.Client.from_service_account_json("api-auth.json")
# oldDf = databaseRead(client)
#
# # sorts DataFrame
# def sortOldDf(oldDf):
#     oldDf['time'] = oldDf['time'].str[0:7]
#     oldDf['time'] = pd.to_datetime(oldDf['time'], format='%I:%M%p').dt.time
#     oldDf['date'] = pd.to_datetime(oldDf['date'], format='%b-%d-%y').dt.date
#     oldDf = oldDf.sort_values(by=['date', 'time'], ascending=[False, False])
#     return oldDf
#
# oldDf = sortOldDf(oldDf)
# data = oldDf[oldDf['ticker'] == 'AMZN']
# print(data)



# import pandas as pd
# df = pd.read_csv('S&P500.csv')
# df = df.sort_values(by=['newsDateLength'], ascending=[False])
# tickerList = df['ticker'].tolist()
#
# i = 0
# while i < len(tickerList):
#     tempTickers = tickerList[i: i+10]
#     i += 10
#     for ticker in tempTickers:
#         print(ticker)
#         #print(tempTickers.get_loc(ticker))
#         print(tempTickers.index(ticker))
#         print(ticker + " (" + str(tempTickers.index(ticker) + 1) + "/" + str(len(tempTickers)) + "): ")# + str(i) + "/100\t" + str(nextTime - startTime) + " elapsed")

# from datetime import datetime
# import pytz
#
# runEndMessage = "\nRun Ended: " + str(datetime.now(pytz.timezone('US/Central')).date()) + " (" + str(
#     datetime.now(pytz.timezone('US/Central')).time().replace(microsecond=0).strftime("%I:%M %p")) + ")\n"
# print(runEndMessage)



# import logging
# from datetime import datetime
# import os
# from emailUpdate import send_email
#
#
# from colorama import Back, Style, Fore
# #logging.info(Fore.GREEN + 'Entering sweeper.py...' + Style.NORMAL)
# log_name = str(datetime.now().date()) + ": GCP.log"
# logging.basicConfig(level=logging.INFO,  handlers=[logging.FileHandler(log_name), logging.StreamHandler()], format='%(message)s')
#
#
# logging.info("25 Tickers to scrape (aap, fffg, ddd)\n")
#
#
#
#
# file = open(log_name, 'r')
# message = file.read()
# os.remove(log_name)
#
# send_email("logs test", message)
#
# file = open('all_GCP.log', 'a')
# file.write(message)
# file.close()



# from google.cloud import bigquery
#
# client = bigquery.Client.from_service_account_json("api-auth.json")
# df = client.query("SELECT date, time, ticker FROM `the-utility-300815.stock_news.SP500`").to_dataframe()
# print(df)


