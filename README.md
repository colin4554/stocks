# Stock News Scraper (and other related, useful functions)

A great way to collect data for investing, sentiment analysis, or other projects!

- Scrape recent news articles for any ticker listed on finviz.com and store the title, full text, summary, keywords, and more!
- Schedule your news scraping and use built in functions to only scrape new articles and based on the frequency of articles for that ticker
- Get the current list of S&P 500 tickers by a simple scraping function
- Email updates each time the scraper runs
- Deploy the repository either locally or on Google Cloud Platform (for free!)

### Deploying locally

-- steps --


### Deploying on Google Cloud Platform (GCP)

-- steps --

TODO:
- anything listed in python
- rss reader for edgar files (or any SEC filings)
- make things into different scripts (initialization function when returned database is [], etc.)



Note:  uninstall pyarrow, install, pyarrow, install bigquery-storage-..., and the problem I faced worked!

More details to follow...


grpcio troubleshooting:
1 person said one dependency might be installing an old version
so try `pip install grpcio` first, and then install requirements.txt

(below) didn't really work
deleted from requirements.txt:

`grpcio==1.34.0
grpcio-tools==1.34.0`