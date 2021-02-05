"""Schedule both local DB and GCP scripts here

Notes:
    - TODO: Get bigquery credentials and enter the database_id before running for GCP (format below)
    - TODO: Setup postgresql table and enter database_id before running for local storage (format below)
    - if scheduled to run every day, it will only scrape tickers that need to be scraped and up to previously scraped article
    - schedule library allows lost of flexibility for scheduling as needed
"""

from gcp_stock_news_scraper import GCPScrape
from local_stock_news_scraper import LocalScrape

import schedule
import time


### ------------------ GCP Scheduling ------------------ ###

def run_GCP():
    # GCP_DATABASE_ID = '`project_name.dataset_name.table_name`'
    GCP_DATABASE_ID = '`the-utility-300815.stock_news.SP500`'
    GCPScrape(GCP_DATABASE_ID)


# # weekday schedule
schedule.every().monday.at("08:30").do(run_GCP)
schedule.every().tuesday.at("08:30").do(run_GCP)
schedule.every().wednesday.at("08:30").do(run_GCP)
schedule.every().thursday.at("08:30").do(run_GCP)
schedule.every().friday.at("08:30").do(run_GCP)


while True:
    schedule.run_pending()
    # 30 second sleep
    time.sleep(30)


### ------------------ Local Scheduling ------------------ ###

#
# def run_local():
#     # LOCAL_DATABASE_ID = "Database Dialect://Username:Password@Server/Name of Database"
#     LOCAL_DATABASE_ID = "postgresql+psycopg2://postgres:2017@localhost/stock-news"
#     LocalScrape(LOCAL_DATABASE_ID)
#
# schedule.every().monday.at("09:30").do(run_local)
# schedule.every().tuesday.at("09:30").do(run_local)
# schedule.every().wednesday.at("09:30").do(run_local)
# schedule.every().thursday.at("09:30").do(run_local)
# schedule.every().friday.at("09:30").do(run_local)
#
#
# while True:
#     schedule.run_pending()
#     # 30 second sleep
#     time.sleep(30)

