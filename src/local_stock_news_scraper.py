"""Local main script, scrape recent news articles for any ticker listed on finviz.com and store the title, full text, summary, keywords, etc.

Inherits gcp_stock_news_scraper to change read/copy database

Notes:
    - sends email update each time it runs, the first the script runs you will need to authorize your google account
"""

from sqlalchemy import create_engine
import pandas as pd
import logging
from datetime import datetime

# parent class
from src.gcp_stock_news_scraper import GCPScrape


class LocalScrape(GCPScrape):

    def __init__(self, database_id):
        """Main Execution: Scrapes tickers that meet criteria, logs data, sends email, updates logs, etc.

        Notes:
            - only database functions are different for local and GCP + log names
            - Schedule this script with schedule_scraper.py
        """
        super().__init__(database_id)
        self.log_name = str(datetime.now().date()) + ": local.log"
        self.file_name = "all_local.log"



    def database_read(self, client):
        """Reads date, time, and ticker data in bigquery datatable for all entries

            Returns:
                postgresql data response as df
            """
        alchemyEngine = create_engine(self.database_id, pool_recycle=3600);

        # Connect to PostgreSQL server
        connect = alchemyEngine.connect();

        # Read data from PostgreSQL database table and load into a DataFrame instance
        df = pd.read_sql("SELECT * FROM \"news2\"", connect);
        pd.set_option('display.expand_frame_repr', False);

        # Close the database connection
        connect.close();

        return df


    def database_copy(self, df):
        """Appends df to postgresql table

        Args:
            df: df that contains article data
        """
        alchemyEngine = create_engine(self.database_id, pool_recycle=3600);

        connect = alchemyEngine.connect();
        # postgreSQLTable = "news";
        postgreSQLTable = "news2";

        try:
            df.to_sql(postgreSQLTable, connect, if_exists='append');
        except ValueError as vx:
            logging.info(vx)
        except Exception as ex:
            logging.info(ex)
        else:
            logging.info("PostgreSQL Table %s has been created successfully." % postgreSQLTable)
        finally:
            connect.close();
            logging.info("Connection Closed")