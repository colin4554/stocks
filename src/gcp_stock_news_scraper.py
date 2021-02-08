"""GCP main script, scrape recent news articles for any ticker listed on finviz.com and store the title, full text, summary, keywords, etc.

Notes:
    - sends email update each time it runs, the first the script runs you will need to authorize your google account
"""

import pandas as pd
from newspaper import Article, nlp
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from google.cloud import bigquery
import logging
import os

from email_update import send_email


class GCPScrape:

    def __init__(self, database_id):
        """Main Execution: Scrapes tickers that meet criteria, logs data, sends email, updates logs, etc.

        Notes:
            - I only turned this into a class to user inheritance for the postrgresql script,
              you can view the old main() function at the bottom
            - schedule this script with schedule_scraper.py
        """
        self.database_id = database_id  # format: '`project_name.dataset_name.table_name`'

        # header for requests
        self.HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

        self.file_name = "../all_GCP.log"
        # creates log file
        self.log_name = str(datetime.now().date()) + ": GCP.log"
        logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(self.log_name), logging.StreamHandler()],
                            format='%(message)s')
        # logs starting time
        logging.info(str(datetime.now(pytz.timezone('US/Central')).date()) + " (" + str(
            datetime.now(pytz.timezone('US/Central')).time().replace(microsecond=0).strftime("%I:%M %p")) + "):\n")

        # don't need to declare all columns for it to populate them
        self.df = pd.DataFrame(columns=['ticker', 'date', 'time', 'link', 'source', 'title', 'error'])

        # big query client, need to include authorization
        self.client = bigquery.Client.from_service_account_json("../api-auth.json")

        # reduce cost by only reading once
        try:
            self.old_df = self.sort_old_df(self.database_read(self.client))
        except:
            logging.info("database_read failed: data does not exist")
            self.old_df = []

        # ticker_list = ['AAPL', 'AMZN', 'GOOG', 'FB', 'MSFT', 'CRM']
        self.ticker_list = self.get_ticker_list(self.old_df)

        logging.info(str(len(self.ticker_list)) + " tickers for current scraping: " + str(self.ticker_list) + "\n")

        # creates DataFrame
        self.df = self.create_df(self.ticker_list, self.df, self.old_df)
        logging.info(self.df)

        self.database_copy(self.client, self.df)

        logging.info("\nRun Ended: " + str(datetime.now(pytz.timezone('US/Central')).date()) + " (" + str(
            datetime.now(pytz.timezone('US/Central')).time().replace(microsecond=0).strftime("%I:%M %p")) + ")\n")

        message = self.commit_logs(self.log_name)
        print(send_email("GCP Stock News Scraping Update", message))


    def fin_viz_table(self, ticker):
        """Gets all article urls from finviz for a specific ticker

        Args:
            ticker: ticker to scrape for

        Returns:
            List of news article urls and date/times
        """
        try:
            url = 'https://finviz.com/quote.ashx?t=' + ticker

            # gets news data from table
            req = requests.get(url, headers=self.HEADERS)
            soup = BeautifulSoup(req.content, 'html.parser')
            news_table = soup.find(id='news-table')

            # splits based on tag for each row of news
            return news_table.findAll('tr')
        except Exception as e:
            logging.info(ticker + " " + str(e))
            return []


    def stop_scrape(self, ticker, old_df):
        """Stops current scraping at last scraped article by checking date and time values of previous scrapes and current news

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

            newsTable = self.fin_viz_table(ticker)
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

                # scraped date of article to analyze
                scrape_date = datetime.combine(date2, time2)

                if scrape_date - trueDate <= timedelta(0):
                    logging.info(ticker + ': previous date: %s scraped date: %s, scrape will stop at index %i' % (trueDate, scrape_date, i))
                    # where to stop (includes buffer of 1, ex: 0)
                    return i

        except Exception as e:
            logging.info("error occurred with stop scrape function " + str(e))
        # if no match, scrape everything
        return 100


    # Yahoo Finance ad for 'Tip Ranks' breaks newspaper3k
    # This function extracts the main text for any yahoo finance article
    # The summary is calculated with the actual newspaper function
    def yahoo_get_text(self, article):
        """Yahoo workaround for extracting article data

        Args:
            article: aritlce newspaper object

        Returns:
            Dictionary of article data
        """
        response = requests.get(article.url, headers= self.HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')

        text = soup.find('div', attrs={'class': 'caas-body'}).text
        summary = nlp.summarize(url=article.url, title=article.title, text=text, max_sents=5)

        # converts list into one paragraph
        summary = ' '. join(sentence for sentence in summary)
        keywords = ", ".join([item for item in article.keywords])

        return {'title': article.title, 'keywords': keywords, 'summary': summary,
                          'full_text': text, 'meta_descr': article.meta_description, 'error': 'yahoo finance workaround'}


    def article_info(self, url):
        """Extracts article data using newspaper

        Args:
            url: url of article

        Returns:
            Dictionary of article data
        """
        try:
            try:
                article = Article(url, browser_user_agent = self.HEADERS['User-Agent'])

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
                if 'finance.yahoo.com' in article.url:
                    return self.yahoo_get_text(article)
                else:
                    keywords = ", ".join([item for item in article.keywords])
                    return {'title': article.title, 'keywords': keywords, 'summary': article.summary, 'full_text': article.text, 'meta_descr': article.meta_description}

        # prints and logs error
        except Exception as e:
            logging.info(article.url + "... article skipped due to error: " + str(e))
            return {'error': 'article skipped'}


    def create_df(self, ticker_list, df, old_df):
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
            news_table = self.fin_viz_table(ticker)

            # gets index to stop at
            stop_index = self.stop_scrape(ticker, old_df)

            # adds row to DataFrame based on date/time
            for i, table_row in enumerate(news_table):
                url = table_row.a['href']
                info = self.article_info(url)

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
    def database_copy(self, client, df):
        """Appends df to bigquery datatable

        Args:
            client: google api connection client
            df: df that contains article data
        """
        try:
            database_id = "the-utility-300815.stock_news.SP500"
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

            job = client.load_table_from_dataframe(df, database_id, job_config=job_config)  # Make an API request.
            job.result()  # Wait for the job to complete.
            logging.info("df has been appended successfully.")

        except Exception as e:
            logging.info(str(datetime.now().replace(microsecond=0)) + ' database_copy Error: ' + str(e))



    def database_read(self, client):
        """Reads data in google bigquery

        Args:
            client: google api connection client

        Returns:
            google bigquery data query response as a DataFrame
        """

        # ---------------- alternate/old way to get all table data  ------------------ #

        # project = "PROJECT_ID"
        # dataset_id = "DATABASE_ID"
        # dataset_ref = bigquery.DatasetReference(project, dataset_id)
        # table_ref = dataset_ref.table("TABLE_ID")
        # table = client.get_table(table_ref)
        # return client.list_rows(table).to_DataFrame()

        # new way saves data by not retrieving any unneccesary columns (1/300 cost)
        # ---------------------------------------------------------------------------- #
        return client.query("SELECT date, time, ticker FROM " + self.database_id).to_dataframe()


    def sort_old_df(self, old_df):
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


    def get_ticker_list(self, old_df):
        """Gets tickers that haven't been scraped in a while by referencing their average published article time span

        Args:
            old_df: df of previously scraped articles

        Returns:
            List of tickers to scrape
        """
        ticker_list = []
        df = pd.read_csv('../S&P500.csv')
        main_ticker_list = [df['ticker'], df['newsDateLength']]

        for i in range(len(main_ticker_list[0])):
            ticker = main_ticker_list[0][i]
            length = main_ticker_list[1][i]

            # if no record exists in database, add to scraping list
            if old_df[old_df['ticker'] == ticker].empty:
                ticker_list += [ticker]
            else:
                # get last scraped date
                date = old_df[old_df['ticker'] == ticker]['date'].iloc[0]
                date_dif = datetime.now().date() - date

                # if the number of days since last scraped is greater than a 4th of the average number of dates on finviz for that ticker, scrape again
                if date_dif.days > length / 4:
                    ticker_list += [ticker]
        return ticker_list




    def commit_logs(self, log_name):
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
        file = open(self.file_name, 'a')
        file.write(message)
        file.close()
        return message
