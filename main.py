#basics1
import os
import ast
import requests
import time
from datetime import datetime, timedelta, timezone
import pytz
import pandas as pd


#tools
import pikepdf
from pdfminer.high_level import extract_text

#Google Cloud
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

# Prepare Google cloud tools
from dotenv import load_dotenv
load_dotenv()

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
G_KEY = os.environ.get('G_KEY')
G_SCOPES = ast.literal_eval(os.environ.get('G_SCOPES')) #to make the string into an array
G_PRJ_ID = os.environ.get('G_PRJ_ID')
G_BUCKET_NAME = os.environ.get('G_BUCKET_NAME')


credentials = service_account.Credentials.from_service_account_file(G_KEY, scopes=G_SCOPES)
storage_client = storage.Client(credentials=credentials, project=G_PRJ_ID)
bigquery_client = bigquery.Client(credentials=credentials, project=G_PRJ_ID)

#Set variables
BASE_URL = "https://www.dnb.no"
JSON_URL = "https://www.dnb.no/portalfront/portal/list/list-files.php?paths=%2Fportalfront%2Fnedlast%2Fno%2Fmarkets%2Fanalyser-rapporter%2Fnorske%2Fanbefalte-aksjer%2F%7Cusename%3DAnbefalte+aksjer%7Ccount%3D52"
CHECK_INTERVAL = 60  # check every minute
DOWNLOAD_DIR = "./temp/"  # current directory
OSLO_TZ = pytz.timezone('Europe/Oslo')
#timezone = OSLO_TZ

def download_file_to_disk(file_url, destination_dir):
    response = requests.get(file_url, stream=True)
    file_name = os.path.basename(file_url)
    file_path = os.path.join(destination_dir, file_name) 

    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return file_path


def remove_restrictions(file_path):
    dir_name = os.path.dirname(file_path)
    base_name = 'clean_' + os.path.basename(file_path)
    output_path = os.path.join(dir_name, base_name)
    try:
        with pikepdf.open(file_path) as pdf:
            pdf.save(output_path)
    except pikepdf.PasswordError:
        print("The PDF is encrypted. Cannot remove restrictions without a password.")
    return output_path


#file_path = clean_local_file_path
#file_path = "C:\\Users\\erong\\source\\repos\\DNB Ukens Aksjer\\temp\\clean_AA231009.pdf"
#bucket_name = G_BUCKET_NAME


#file_path = "C:\\Users\\erong\\source\\repos\\DNB Ukens Aksjer\\temp\\clean_AA231009.pdf"
#blob_name = 'clean_AA231009.pdf'
#blob = storage_client.bucket('ukens_aksjer').blob(blob_name)
#lob.upload_from_filename(file_path)


def upload_to_gcs(file_path, bucket_name, storage_client):
    blob_name = file_path.split('\\')[-1]  # Assuming last part of file_path is filename

    blob = storage_client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(file_path)

    gcs_file_path = f"gs://{bucket_name}/{blob_name}"
    return gcs_file_path


def extract_text_from_pdf(file_path):
    text = extract_text(file_path)
    return text

def wait_until_9_am(timezone):
    now = datetime.now(timezone)
    if now.hour < 9:
        next_9_am = now.replace(hour=9, minute=0, second=0, microsecond=0)
        delta_seconds = (next_9_am - now).total_seconds()
        time.sleep(delta_seconds)

def wait_until_next_monday(timezone):
    today = datetime.now(timezone)
    
    # If today is already Monday, return immediately
    if today.weekday() == 0:
        return
    
    # Calculate the date of the next Monday
    next_monday = today + timedelta((0-today.weekday() + 7) % 7)
    
    # Set the time to 00:00:00
    next_monday = next_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate sleep duration in seconds
    delta_seconds = (next_monday - today).total_seconds()
    
    time.sleep(delta_seconds)

def append_to_bigquery(df, table_name, project_id):
    df.to_gbq(table_name, project_id=project_id, if_exists='append', progress_bar=True)

def create_dataframe(URL, GCS_Path, text, load_date_local):
    data = {
        'URL': [URL],
        'GCS_Path': [GCS_Path],
        'text': [text],
        'load_date': [load_date_local]

    }
    return pd.DataFrame(data)


def main():
    # If it is not monday, wait til next monday 00:00 (oslo time) 
    wait_until_next_monday(OSLO_TZ)

    wait_until_9_am(OSLO_TZ)

    # On the first run, just mark the most recent entry as the last known entry
    response = requests.get(JSON_URL)
    json_data = response.json()
    last_downloaded = json_data["data"][0]["path"]

    while True:
        now = datetime.now(OSLO_TZ)

        # End the script if it's past 19:00 in Oslo/Norway time
        if now.hour >= 19:
            break

        # Fetch the JSON
        response = requests.get(JSON_URL)
        json_data = response.json()

        # If there's a new entry
        newest_entry = json_data["data"][0]
        newest_file_path = newest_entry["path"]
        
        if last_downloaded != newest_file_path:
            # Download the new file
            local_file_path = download_file_to_disk(BASE_URL + newest_file_path, DOWNLOAD_DIR)
            print(f"Downloaded {local_file_path}")

            # Remove restrictions from the file
            clean_local_file_path = remove_restrictions(local_file_path)

            # Upload file to GCS
            gcs_path = upload_to_gcs(clean_local_file_path, G_BUCKET_NAME,storage_client)
            print(f"Uploaded to GCS at {gcs_path}")

            # Extract text from the file
            text = extract_text_from_pdf(local_file_path)
            print(f"Extracted Text: {text}")

            append_to_bigquery(create_dataframe(BASE_URL + newest_file_path, gcs_path, text, datetime.now(OSLO_TZ)), 'alt.dnb_ukens_aksjer', G_PRJ_ID)

            # Update the last downloaded path
            last_downloaded = newest_file_path

        # Sleep for the interval before checking again
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()




def save_text_to_file(text, filename="output.txt"):
    
    file_path = f"./temp/{filename}"
    
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(text)
    
    return file_path

save_text_to_file(text, "test.txt")