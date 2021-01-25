import pandas as pd
from newspaper import Article, nlp
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from google.cloud import bigquery
import schedule
import logging
import os

from emailUpdate import send_email

# header for requests
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


# TODO: Current Problems

# Will incur a bigquery data loading cost after free trial
# ^ could use Cloud Storage as intermediary to eliminate cost

# Websites with some text, but most behind a paywall are still scraped
# ^ so full text, summary, etc. is sparse, but we don't know which articles

# redact sensitive information and make public on github
# ^ will also need to clean up code and add things (initialize function, pipeline, etc.)



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


def fin_viz_table(ticker):
    """Gets all article urls from finviz for a specific ticker

    Args:
        ticker: ticker to scrape for

    Returns:
        List of news article urls and date/times
    """
    url = 'https://finviz.com/quote.ashx?t=' + ticker

    # gets news data from table
    req = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(req.content, 'html.parser')
    news_table = soup.find(id='news-table')

    # splits based on tag for each row of news
    return news_table.findAll('tr')


# Function to speed up scraping and reduce duplicates
# reads old DataFrame and takes most recent date and time scraped
# then finds corresponding index to stop scraping at
def stop_scrape(ticker, old_df):
    """Stops current scraping at last scraped article by checking date and time values

    Args:
        ticker: ticker to scrape for
        old_df: df of previously scraped articles

    Returns:
        Index number to stop scraping or 100 if no previous records/error occurs
    """
    try:
        # copy is needed to avoid copy of slice error (SettingWithCopyWarning)
        old_df = old_df[old_df['ticker'] == ticker].copy()

        # gets most recent date and time from last scrape
        date = old_df['date'].iloc[0]
        time = old_df['time'].iloc[0]

        # this is last scraped article date/time
        trueDate = datetime.combine(date, time)

        newsTable = fin_viz_table(ticker)
        # cycles through articles and finds safe index to scrape too based on oldDf
        for i, table_row in enumerate(newsTable):

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
                logging.info(ticker + ': previous date: %s scraped date: %s, scrape will stop at index %i' % (trueDate, scrapeDate, i))
                # where to stop (includes buffer of 1, ex: 0)
                return i

    except Exception as e:
        logging.info("error occurred with stop scrape function " + str(e))
    # if no match, scrape everything
    return 100


# Yahoo Finance ad for 'Tip Ranks' breaks newspaper3k
# This function extracts the main text for any yahoo finance article
# The summary is calculated with the actual newspaper function
def yahoo_get_text(article):
    """Yahoo workaround for extracting article data

    Args:
        article: aritlce newspaper object

    Returns:
        Dictionary of article data
    """
    response = requests.get(article.url, headers= HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')

    text = soup.find('div', attrs={'class': 'caas-body'}).text
    summary = nlp.summarize(url=article.url, title=article.title, text=text, max_sents=5)

    # converts list into one paragraph
    summary = ' '. join(sentence for sentence in summary)
    keywords = ", ".join([item for item in article.keywords])

    return {'title': article.title, 'keywords': keywords, 'summary': summary,
                      'full_text': text, 'meta_descr': article.meta_description, 'error': 'yahoo finance workaround'}


def article_info(url):
    """Extracts article data using newspaper

    Args:
        url: url of article

    Returns:
        Dictionary of article data
    """
    try:
        try:
            article = Article(url, browser_user_agent = HEADERS['User-Agent'])

            # fixes issue with bloomberg
            if 'bloomberg.com' in article.url:
                article.url = article.url.replace('www.bloomberg.com', 'www.bloombergquint.com')
            article.download()
            article.parse()

        except Exception as e:
            logging.info(e)
            # sometimes we need to use googlebot if an error occurs
            article = Article(url, browser_user_agent='Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)')
            article.download()
            article.parse()
            logging.info(article.url[8:40] + "... article scraped with googlebot")
        finally:
            article.nlp()

            # prevents Yahoo finance articles from being scraped incorrectly
            if 'finance.yahoo.com' in article.url :
                return yahoo_get_text(article)
            else:
                keywords = ", ".join([item for item in article.keywords])
                return {'title': article.title, 'keywords': keywords, 'summary': article.summary, 'full_text': article.text, 'meta_descr': article.meta_description}

    # prints and logs error
    except Exception as e:
        logging.info(article.url + "... article skipped due to error: " + str(e))
        return {'error': 'article skipped'}


def create_df(ticker_list, df, old_df):
    """Stops current scraping at last scraped article by checking date and time values

    Args:
        ticker_list: ticker to scrape for
        df: empty df that will contain article data
        old_df: df of previously scraped articles

    Returns:
        Index number to stop scraping or 100 if no previous records/error occurs
    """
    start_time = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')
    for ticker in ticker_list:
        time.sleep(5)

        # gets finviz.com news table
        news_table = fin_viz_table(ticker)

        # gets index to stop at
        stop_index = stop_scrape(ticker, old_df)

        # adds row to DataFrame based on date/time
        for i, table_row in enumerate(news_table):
            url = table_row.a['href']
            info = article_info(url)

            # shows progress to user
            if i % 20 == 0:
                next_time = datetime.strptime(datetime.now().strftime('%H:%M:%S'), '%H:%M:%S')

                logging.info(ticker + " (" + str(ticker_list.index(ticker) + 1) + "/" + str(len(ticker_list)) + "): " + str(i) + "/100\t" + str(next_time - start_time) + " elapsed")
                time.sleep(1)

            if len(table_row.td.text.split()) == 1:
                row = {**{'ticker': ticker, 'time': table_row.td.text, 'link': url, 'source': table_row.span.text[1:],
                          'title': table_row.a.text}, **info}
            else:
                row = {**{'ticker': ticker, 'date': table_row.td.text.split()[0], 'time': table_row.td.text.split()[1],
                          'link': table_row.a['href'], 'source': table_row.span.text[1:], 'title': table_row.a.text}, **info}
            df = df.append(row, ignore_index=True)

            if i == stop_index:
                logging.info(ticker + ": scrape stopped at %i\n" % stop_index)
                break

    # TIMES are in EST Time
    # fills in dates
    df['date'] = df['date'].ffill()
    df['scrape_date'] = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
    return df


# appends DataFrame to bigquery SP500 data table
def database_copy(client, df):
    """Appends df to bigquery datatable

        Args:
            client: google api connection client
            df: df that contains article data
    """
    try:
        table_id = "the-utility-300815.stock_news.SP500"
        job_config = bigquery.LoadJobConfig(

        schema=[
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("link", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("full_text", "STRING"),
            bigquery.SchemaField("keywords", "STRING"),
            bigquery.SchemaField("meta_descr", "STRING"),
            bigquery.SchemaField("summary", "STRING"),
            bigquery.SchemaField("error", "STRING"),
            bigquery.SchemaField("scrape_date", "STRING")
        ])

        job = client.load_table_from_DataFrame(df, table_id, job_config=job_config)  # Make an API request.
        job.result()  # Wait for the job to complete.
        logging.info("df has been appended successfully.")

    except Exception as e:
        logging.info(str(datetime.now().replace(microsecond=0)) + ' database_copy Error: ' + str(e))



def database_read(client):
    """Reads date, time, and ticker data in bigquery datatable for all entries

        Args:
            client: google api connection client

        Returns:
            Index number to stop scraping or 100 if no previous records/error occurs
        """

    # alternate/old way to get all table data

    # project = "the-utility-300815"
    # dataset_id = "stock_news"
    # dataset_ref = bigquery.DatasetReference(project, dataset_id)
    # table_ref = dataset_ref.table("SP500")
    # table = client.get_table(table_ref)
    # return client.list_rows(table).to_DataFrame()

    # saves data by not retrieving any unneccesary columns (1/300 cost)
    return client.query("SELECT date, time, ticker FROM `the-utility-300815.stock_news.SP500`").to_DataFrame()


def sort_old_df(old_df):
    """Sorts DataFrame by time and date values

        Args:
            old_df: df of previously scraped articles

        Returns:
            DataFrame sorted by time and date with most recent date/times first
        """
    old_df['time'] = old_df['time'].str[0:7]
    old_df['time'] = pd.to_datetime(old_df['time'], format='%I:%M%p').dt.time
    old_df['date'] = pd.to_datetime(old_df['date'], format='%b-%d-%y').dt.date
    old_df = old_df.sort_values(by=['date', 'time'], ascending=[False, False])
    return old_df


def get_ticker_list(old_df):
    """Gets tickers that haven't been scraped in a while by referencing their average published article time span

            Args:
                old_df: df of previously scraped articles

            Returns:
                List of tickers to scrape
            """
    ticker_list = []
    df = pd.read_csv('S&P500.csv')
    main_ticker_list = [df['ticker'], df['newsDateLength']]

    for i in range(len(main_ticker_list[0])):
        ticker = main_ticker_list[0][i]
        length = main_ticker_list[1][i]

        # if no record exists in database, add to scraping list
        if old_df['ticker'][old_df['ticker'] == ticker].sum() == 0:
            ticker_list += [ticker]
        else:
            # get last scraped date
            date = old_df[old_df['ticker'] == ticker]['date'].iloc[0]
            date_dif = datetime.now().date() - date

            # if the number of days since last scraped is greater than a 4th of the average number of dates on finviz for that ticker, scrape again
            if date_dif.days > length / 4:
                ticker_list += [ticker]
    return ticker_list




def commit_logs(log_name):
    """Commits logs to the master log file

        Args:
            log_name: name of this run's log (date: GCP.log)

        Returns:
            All logs from recent run
        """
    # gets logs from current run, then deletes file
    file = open(log_name, 'r')
    message = file.read()
    os.remove(log_name)

    # adds logs from current run to master log
    file = open('all_GCP.log', 'a')
    file.write(message)
    file.close()
    return message


# --- Main Execution --- #

# email message update body text

def main():
    """Sorts DataFrame by time and date values

        Args:
            old_df: df of previously scraped articles

        Returns:
            DataFrame sorted by time and date with most recent date/times first
    """
    # big query client, need to include authorization
    client = bigquery.Client.from_service_account_json("api-auth.json")

    # creates log file
    log_name = str(datetime.now().date()) + ": GCP.log"
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(log_name), logging.StreamHandler()], format='%(message)s')

    # logs starting time
    logging.info(str(datetime.now(pytz.timezone('US/Central')).date()) + " (" + str(
        datetime.now(pytz.timezone('US/Central')).time().replace(microsecond=0).strftime("%I:%M %p")) + "):\n")

    # don't need to declare all columns for it to populate them
    df = pd.DataFrame(columns=['ticker', 'date', 'time', 'link', 'source', 'title'])

    # reduce cost by only reading once
    try:
        old_df = database_read(client)
        old_df = sort_old_df(old_df)
    except:
        logging.info("database_read failed: data does not exist")
        old_df = []

    # ticker_list = ['AAPL', 'AMZN', 'GOOG', 'FB', 'MSFT', 'CRM']
    ticker_list = get_ticker_list(old_df)

    logging.info(str(len(ticker_list)) + " tickers for current scraping: " + str(ticker_list) + "\n")

    # creates DataFrame
    df = create_df(ticker_list, df, old_df)
    logging.info(df.head())

    database_copy(client, df)

    logging.info("\nRun Ended: " + str(datetime.now(pytz.timezone('US/Central')).date()) + " (" + str(
        datetime.now(pytz.timezone('US/Central')).time().replace(microsecond=0).strftime("%I:%M %p")) + ")\n")

    message = commit_logs(log_name)
    print(send_email("GCP Stock News Scraping Update", message))



### ------------------ Initialization Scipt ------------------ ###

# initial setup, scrapes all tickers in batches of 10.  Hopefully I can examine any errors later, and use the get_ticker_list function to patch any up

# df = pd.read_csv('S&P500.csv')
# df = df.sort_values(by=['newsDateLength'], ascending=[False])
#
# # first 140 tickers worked, so starting after (first 60 killed due to OOM, those after had swap installed)
# tickerList = df['ticker'][140:].tolist()
#
# # read bigquery once
# client = bigquery.Client.from_service_account_json("api-auth.json")
# oldDf = database_read(client)
#
# i = 0
# while i < len(tickerList):
#     try:
#         tempTickers = tickerList[i:i+10]
#         i += 10
#         main(tempTickers, oldDf)
#     except Exception as e:
#         print("Error occurred at highest abstraction: " + str(e))


### ------------------ Scheduling ------------------ ###


# schedule.every(2).seconds.do(main)


# weekday schedule

#main()

schedule.every().monday.at("08:30").do(main)
schedule.every().tuesday.at("08:30").do(main)
schedule.every().wednesday.at("08:30").do(main)
schedule.every().thursday.at("08:30").do(main)
schedule.every().friday.at("08:30").do(main)


while True:
    schedule.run_pending()
    # 30 second sleep
    time.sleep(30)