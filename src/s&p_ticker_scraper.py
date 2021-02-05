"""Scrapes current list of S&P 500 companies from Wikipedia

Notes:
    - stores data in S&P500_Tickers.csv
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs


def __init__():

  # makes get (read) request
  HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
  url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
  request = requests.get(url, headers=HEADERS)

  # parses content, finds table, creates list of rows
  soup = bs(request.content, 'html.parser')
  table = soup.find(id='constituents')
  rows = table.findAll('tr')

  df = pd.DataFrame(columns=['ticker', 'company name', 'sector', 'sub-industry'])

  # goes through each row and assigns items to correct columns
  for i, row in enumerate(rows[1:]):
    items = row.findAll('td')
    dic = {'ticker' : items[0].text[0:-1], 'company name' : items[1].text, 'sector' : items[3].text, 'sub-industry' : items[4].text, 'date first added' : items[6].text}
    df = df.append(dic, ignore_index=True)

  print(df)

  df.to_csv('S&P500_Tickers.csv', index=False)

