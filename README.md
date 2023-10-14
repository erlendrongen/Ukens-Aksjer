# Read thousands of PDFs and manually type into Excel?
# Or automate the job with python and ChatGPT

## Here is how to automatically extract stock recommendations from PDF documents

### Background
DNB Bank ASA releases [stock recommendations](https://www.dnb.no/bedrift/markets/analyser/arkiv/anbefalteaksjer.html) for the Oslo Stock exchange every monday between 09:00 and 15:30. Automatic extraction of these recommendations allows for automated notifications at the minute they are released and backtesting to see if the stock recommendations are really as good as they claim to be.

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

