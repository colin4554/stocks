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

from datetime import datetime
import requests
from bs4 import BeautifulSoup


import pandas as pd
from sqlalchemy import create_engine

def sqlRead():
    alchemyEngine = create_engine('postgresql+psycopg2://postgres:2017@localhost/stock-news', pool_recycle=3600);

    # Connect to PostgreSQL server
    connect = alchemyEngine.connect();

    # Read data from PostgreSQL database table and load into a DataFrame instance
    df = pd.read_sql("SELECT * FROM \"news2\"", connect);
    pd.set_option('display.expand_frame_repr', False);

    # Close the database connection
    connect.close();

    return df

ticker = 'AMZN'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

url = 'https://finviz.com/quote.ashx?t=' + ticker

oldDf = sqlRead()

# gets news data from table
req = requests.get(url, headers=HEADERS)
soup = BeautifulSoup(req.content, 'html.parser')
news_table = soup.find(id='news-table')

# splits based on tag for each row of news
news_tr = news_table.findAll('tr')

oldDf = oldDf[oldDf['ticker'] == ticker]

# sorts dataframe
oldDf['time'] = oldDf['time'].str.slice(start=0, stop=7)
oldDf['time'] = pd.to_datetime(oldDf['time'], format='%I:%M%p').dt.time

oldDf = oldDf.sort_values(by=['date', 'time'], ascending=[False, False])

# gets most recent date and time from last scrape
date = oldDf['date'].iloc[0]
date = datetime.strptime(date, '%b-%d-%y')

time = oldDf['time'].iloc[0]
print(time)
#time = pd.Timestamp(time).time()
#print(time)


