import psycopg2
from psycopg2 import Error
import pandas as pd
import os
import newspaper
from newspaper import Article, nlp
import time
import requests
from bs4 import BeautifulSoup
import nltk
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pytz

# Current Problems
# Websites with some text, but most behind a paywall are still scraped
# ^ so full text, summary, etc. is sparse, but we don't know which articles



# --------------------------- if you need to install punkt ----------------------------

# import nltk
# import ssl
#
# try:
#     _create_unverified_https_context = ssl._create_unverified_context
# except AttributeError:
#     pass
# else:
#     ssl._create_default_https_context = _create_unverified_https_context
#
# nltk.download()

# -------------------------------------------------------------------------------------


# Function to speed up scraping and reduce duplicates
# reads old dataframe and takes most recent date and time scraped
# then finds corresponding index to stop scraping at
def stopScrape(ticker, oldDf, HEADERS):
    # creates url
    url = 'https://finviz.com/quote.ashx?t=' + ticker

    # gets news data from table
    req = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(req.content, 'html.parser')
    news_table = soup.find(id='news-table')

    # splits based on tag for each row of news
    news_tr = news_table.findAll('tr')

    # gets most recent date and time from last scrape
    oldDf = oldDf.sort_values(by=['date', 'time'], ascending=[False, False])

    date = oldDf[oldDf['ticker'] == ticker]['date'].iloc[0]
    date = datetime.strptime(date, '%b-%d-%y')
    time = str(oldDf[oldDf['ticker'] == ticker]['time'].iloc[0])
    if time[-1] != "M":
        time = time[0:-2]
    time = datetime.strptime(time, '%I:%M%p').time()

    # this is last scraped article date/time
    trueDate = datetime.combine(date, time)

    # cycles through articles and finds safe index to scrape too based on oldDf
    for i, table_row in enumerate(news_tr):

        # scrapes current date and time from news item.  Date will hold over
        if len(table_row.td.text.split()) == 1:
            # weird characters at the end, not spaces couldn't be removed any other way
            time2 = datetime.strptime(table_row.td.text[0:-2], '%I:%M%p').time()
        else:
            # date2 =
            date2 = datetime.strptime(table_row.td.text.split()[0], '%b-%d-%y')
            time2 = datetime.strptime(table_row.td.text.split()[1], '%I:%M%p').time()

        # scraped date of aritcle to analyze
        scrapeDate = datetime.combine(date2, time2)

        if scrapeDate - trueDate <= timedelta(0):
            message = ticker + ': previous date: %s scraped date: %s, scrape will stop at index %i' % (trueDate, scrapeDate, i + 1)
            print(message)
            file.write("\n" + message)
            # buffer of one adds a duplicate
            return i + 1
    # if no match, scrape everything
    return 100


# Yahoo Finance ad for 'Tip Ranks' breaks newspaper3k
# This function extracts the main text for any yahoo finance article
# The summary is calculated with the actual newspaper function
def yahoo_get_text(article):
    response = requests.get(article.url, headers= {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'})
    soup = BeautifulSoup(response.content, 'html.parser')
    text = soup.find('div', attrs={'class': 'caas-body'}).text
    summary = nlp.summarize(url=article.url, title=article.title, text=text, max_sents=5)
    # converts list into one paragraph
    summary = ' '. join(sentence for sentence in summary)
    return {'title': article.title, 'keywords': article.keywords, 'summary': summary,
                      'full_text': text, 'meta descr': article.meta_description, 'error': 'yahoo finance workaround'}


def articleInfo(url):
    try:
        try:
          # old_browser_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
          article = Article(url, browser_user_agent = 'Mozilla/5.0')
          # gets around bloomberg paywall
          if 'bloomberg.com' in article.url:
              article.url = article.url.replace('www.bloomberg.com', 'www.bloombergquint.com')
          article.download()
          article.parse()
        except:
            # sometimes we need to use the googlebot to get around a paywall
            article = Article(url,browser_user_agent="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")
            print(article.url[8:40] + "... article scraped with googlebot")
            article.download()
            article.parse()
        finally:
          article.nlp()

          # Yahoo finance articles are scraped incorrectly, so this is a workaround
          if 'finance.yahoo.com' in article.url :
            return yahoo_get_text(article)
          else:
            return {'title': article.title, 'keywords' : article.keywords, 'summary' : article.summary, 'full_text' : article.text, 'meta descr' : article.meta_description}

    except Exception as e:
        print(article.url[8:40] + "... article skipped due to error: " + str(e)) #(%i out of %i)" % (i, len(list)))
        file.write("\n" + article.url + "... article skipped due to error: " + str(e))
        return {'error': 'article skipped'}


def createDF(tickerlist, df, HEADERS):
    startTime = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')
    for ticker in tickerlist:
        time.sleep(1)

        # creates url
        url = 'https://finviz.com/quote.ashx?t=' + ticker

        # gets news data from table
        req = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(req.content, 'html.parser')
        news_table = soup.find(id='news-table')

        # splits based on tag for each row of news
        news_tr = news_table.findAll('tr')

        # gets index to stop at
        oldDf = sqlRead()
        stopIndex = stopScrape(ticker, oldDf, HEADERS)

        # adds row to dataframe based on date/time
        for i, table_row in enumerate(news_tr):
            url = table_row.a['href']
            info = articleInfo(url)

            # shows progress to user
            if i % 20 == 0:
                nextTime = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')
                print(ticker + " (" + str(tickerlist.index(ticker) + 1) + "/" + str(len(tickerlist)) + "): " + str(i) + "/100\t" + str(nextTime - startTime) + " elapsed")
                time.sleep(.5)

            if len(table_row.td.text.split()) == 1:
                row = {**{'ticker': ticker, 'time': table_row.td.text, 'link': url, 'source': table_row.span.text[1:],
                          'title': table_row.a.text}, **info}
            else:
                row = {**{'ticker': ticker, 'date': table_row.td.text.split()[0], 'time': table_row.td.text.split()[1],
                          'link': table_row.a['href'], 'source': table_row.span.text[1:], 'title': table_row.a.text},
                       **info}
            df = df.append(row, ignore_index=True)

            if i == stopIndex:
                print(ticker + ": scrape stopped at %i" %stopIndex)
                break

    # TIMES are in EST Time
    # fills in dates
    df['date'] = df['date'].ffill()
    df['scrape_date'] = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
    return df


# function to put data into postgresql database
def sqlCopy(df):
    # create_engine('Database Dialect://Username:Password@Server/Name of Database)
    alchemyEngine = create_engine('postgresql+psycopg2://postgres:2017@localhost/stock-news', pool_recycle=3600);

    connect = alchemyEngine.connect();
    #postgreSQLTable = "news";
    postgreSQLTable = "news2";

    try:
        df.to_sql(postgreSQLTable, connect, if_exists='append');
    except ValueError as vx:
        print(vx)
        file.write("\n" + str(datetime.now().replace(microsecond=0)) + ' databaseCopy Error: ' + str(vx))
    except Exception as ex:
        print(ex)
        file.write("\n" + str(datetime.now().replace(microsecond=0)) + ' databaseCopy Error: ' + str(ex))
    else:
        print("PostgreSQL Table %s has been created successfully." % postgreSQLTable);
    finally:
        connect.close();
        print("Connection Closed")


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

# --- Main Execution --- #

# file to log errors (w - write, a - append)
file = open("records.txt", "a")
file.write("\n" + str(datetime.now().date()) + " (" + str(datetime.now().time().replace(microsecond=0)) + "):")

# header for newspaper
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

# don't need to declare all columns for it to populate them
df = pd.DataFrame(columns=['ticker', 'date', 'time', 'link', 'source', 'title'])

# CRM = salesforce
tickerlist = ['AAPL', 'AMZN', 'GOOG', 'FB', 'MSFT', 'CRM']
# tickerlist = ['AAPL']
# change replace to append

# creates dataframe
df = createDF(tickerlist, df, HEADERS)
print(df)

sqlCopy(df)

df = sqlRead()
print(df)
file.write("\nRun Ended\n")
file.close()
