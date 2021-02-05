# Stock News Scraper (and other related, useful functions)

_A great way to collect data for investing, sentiment analysis, or other projects!_

- Scrape recent news articles for any ticker listed on finviz.com and store the title, full text, summary, keywords, and/or other data
- Schedule your news scraping and use built in functions to only scrape when necessary and avoid duplicate scraping
- Get the current list of S&P 500 tickers by a simple scraping function
- Receive email updates each time the scraper runs with informative logs
- Deploy the repository + database either locally or on Google Cloud Platform (for free!)


### Running Scripts
- Clone/download repository into desired directory
- Run ```pip install -r requirements.txt```
- If you want to run this package completely in the cloud, [set up a free GCP f1-micro instance](https://medium.com/@hbmy289/how-to-set-up-a-free-micro-vps-on-google-cloud-platform-bddee893ac09)

### Deploying database locally

- Create local postgresql table
- Set ```LOCAL_DATABASE_ID``` to postgresql table id
- Use local_stock_news_scraper.py


### Deploying database on Google Cloud Platform (GCP) as BigQuery

- Set up a google cloud platform account
- Create a BigQuery table
- Download credentials for BigQuery API
- Set ```GCP_DATABASE_ID``` to BigQuery table id
- Use gcp_stock_news_scraper.py



_Disclaimer: This project was undertaken for learning and personal use.  Please be respectful of websites when scraping._