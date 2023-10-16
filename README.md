# Read thousands of PDFs and manually type into Excel?
# Or automate the job with python and ChatGPT

## Here is how to automatically extract stock recommendations from PDF documents

### Background
DNB Bank ASA releases [stock recommendations](https://www.dnb.no/bedrift/markets/analyser/arkiv/anbefalteaksjer.html) for the Oslo Stock exchange every monday between 09:00 and 15:30. Automatic extraction of these recommendations allows for automated notifications at the minute they are released and backtesting to see if the stock recommendations are really as good as they claim to be.

In Google Chrome, if you click ctrl+shift+j and go to network and refresh the page, you should see the API call that produces a json with link to all the PDFs. We use this URL since it is much easier than manouvering HTML to web-scrape the PDF document URLS.

URL: https://www.dnb.no/portalfront/portal/list/list-files.php?paths=%2Fportalfront%2Fnedlast%2Fno%2Fmarkets%2Fanalyser-rapporter%2Fnorske%2Fanbefalte-aksjer%2F%7Cusename%3DAnbefalte+aksjer%7Ccount%3D52

### What the scripts do
 * simple.py - easy to use demo script for downloading PDF's, extracting text and extracting structured information using ChatGPT from text
 * Main.py - fully working ETL that picks up new PDF's extracts structured information using ChatGPT from text and writes back to BigQuery
 * output.sql - a BigQuery SQL script that organizes the data and joins stock tickers on to the extracted recommendations



### simple.py
* Downloads past recommendation PDFs
* Removes any PDF read-locks
* Extracts the text
* Stores text as txt-files
* ChatGPT extracts any portfolio changes
* ChatGPT extracts last weeks portfolio holings

### Main.py
This is a fully functional ETL script that automatically checks for new recommendations every 60 seconds.

* Checks every 1 minute for new recommendations
* Removes any PDF read-locks
* Extracts the text
* Stores text in BigQuery
* Stores the PDF ing GCS
* ChatGPT extracts any portfolio changes
* ChatGPT extracts last weeks portfolio holings
* Stores the resulting datasets in GCS

### output.sql
This SQL will join the company names to a list of 

* Checks every 1 minute for new recommendations
* Removes any PDF read-locks
* Extracts the text
* Stores text in BigQuery
* Stores the PDF ing GCS
* ChatGPT extracts any portfolio changes
* ChatGPT extracts last weeks portfolio holings
* Stores the resulting datasets in GCS
