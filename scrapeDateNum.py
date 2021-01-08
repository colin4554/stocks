import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime



df = pd.read_csv('S&P500.csv')

# I added BRK-A and BRK-B instead of just BRK
mainTickerList = df['ticker']
df['newsDateLength'] = ""

startTime = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

for i, ticker in enumerate(mainTickerList):

    try:
        time.sleep(1)

        # creates url
        url = 'https://finviz.com/quote.ashx?t=' + ticker

        # gets news data from table
        req = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(req.content, 'html.parser')
        news_table = soup.find(id='news-table')

        # splits based on tag for each row of news
        news_tr = news_table.findAll('tr')

        dateList = []
        # adds row to dataframe based on date/time
        for j, table_row in enumerate(news_tr):
            if len(table_row.td.text.split()) != 1:
                date = datetime.strptime(table_row.td.text.split()[0], '%b-%d-%y').date()
                dateList += [date]

        dif = dateList[0] - dateList[-1]
        nextTime = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')
        print("(" + str(i+1) + "/" + str(len(mainTickerList)) + ") " + ticker + ": " + str(dif.days) +"     \t" + str(nextTime - startTime) + " elapsed")
        df['newsDateLength'].iloc[i] = dif.days

    except Exception as e:
        print("Error occured with " + ticker + ": " + str(e))

df.to_csv('S&P500.csv', index=False)