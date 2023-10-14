#basics1
import os
import re
import ast
import requests
import time
from datetime import datetime, timedelta, timezone
import pytz
import pandas as pd
import json

from dotenv import load_dotenv
load_dotenv()

#OpenAI
import openai
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

#tools
import pikepdf
from pdfminer.high_level import extract_text

import pyperclip

#Google Cloud
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

# Prepare Google cloud tools

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

def clean_json_string(s):
    # The pattern looks for the outermost curly brackets and captures the content
    match = re.search(r'(\{.*\})', s)
    if match:  # If a match is found
        return match.group(1)  # Return the captured content
    else:
        raise ValueError("No valid JSON structure found in the string")


def get_json_from_text(text):
    # Split the text
    cropped_text = text.split("Figure 2", 1)[0]
    #pyperclip.copy(text)


    #cropped_text = text.split(": Anbefalinger", 1)[0]



    #Prompt 1
    prompt1 = """Teksten under er hentet ut fra en PDF-fil. Du skal hente data fra tabellen "Figure 1: Anbefalte aksjer" og returnere svaret som JSON. 
    
    Returner KUN json, ingen annen tekst! 

    JSON format:

    {
    "Aksjer": [
        {
        "Selskap": "Statoil",
        "Kurs_Inn": 22.19
        },
        {
        "Selskap": "Equinor",
        "Kurs_Inn": 353.40
        },
        {etc..},
        {etc..}
        ]
    }
    """



    sys = {"role": "system", "content": prompt1}
    cr_text = {"role": "user", "content": cropped_text}

    

    meldinger=[]
    meldinger.append(sys)
    meldinger.append(cr_text)


    completion = openai.ChatCompletion.create(model="gpt-4", messages = meldinger)

    print(completion.choices[0].message.content)


    #Prompt 2
    prompt2 = """Du skal nå finne sekslapene som har blit lagt til og tatt ut av porteføljen!
    Du finner eventuelle endringer tidlig i teksten "Endringer denne uken", "Aksjer inn" og "Aksjer ut". PS! Kan også ha ingen endringer.

    Lever svaret som JSON på følgende format, ingen tekst før eller etter.

    {
    "Endringer_denne_uken": [
        {
        "Endring": "Aksjer inn",
        "Selskap": "Equinor",
        },
        {
        "Endring": "Aksjer ut",
        "Selskap": "Ingen",
        },
        {etc..},
        {etc..}
        ]
    }

    """


    sys = {"role": "system", "content": prompt2}
    cr_text = {"role": "user", "content": cropped_text}

    meldinger2=[]
    meldinger2.append(sys)
    meldinger2.append(cr_text)


    completion2 = openai.ChatCompletion.create(model="gpt-4", messages = meldinger2)

    print(completion2.choices[0].message.content)


    try:
        json_obj = json.loads(clean_json_string(completion.choices[0].message.content))
        df_aksjer = pd.DataFrame(json_obj['Aksjer'])
        
        if len(df_aksjer) > 4:
            df_aksjer.iloc[:, :2]
            new_column_names = ['Selskap', 'Kurs_Inn']
            df_aksjer.columns = new_column_names
            df_aksjer = df_aksjer[['Selskap','Kurs_Inn']]
            df_aksjer['Kurs_Inn'] = pd.to_numeric(df_aksjer['Kurs_Inn'], errors='coerce')
        else:
            df_aksjer = pd.DataFrame()
    except:
            df_aksjer = pd.DataFrame()


    try:
        
        json_obj = json.loads(clean_json_string(completion2.choices[0].message.content))
        df_endring = pd.DataFrame(json_obj['Endringer_denne_uken'])
        
        if len(df_endring) > 1:
            df_endring.iloc[:, :2]
            new_column_names = ['Endring', 'Selskap']
            df_endring.columns = new_column_names
            df_endring = df_endring[['Endring', 'Selskap']]
            df_endring = df_endring[df_endring['Selskap'].str.lower() != 'ingen']
            if len(df_endring) == 0:
                df_endring = pd.DataFrame()
            
        else:
            df_endring = pd.DataFrame()
    except:
            df_endring = pd.DataFrame()

    return df_aksjer, df_endring



def delete_files_from_directory(directory):
    """
    Delete all files from the given directory.
    
    Parameters:
    - directory (str): Path to the directory.
    """
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist!")
        return

    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")



def extract_date_from_filename(filename: str) -> datetime:
    """
    Extracts date from the given filename and returns it as a datetime object.

    Args:
    - filename (str): Filename containing the date.

    Returns:
    - datetime: Extracted date.
    """
    match = re.search(r'AA(\d{2})(\d{2})(\d{2})\.pdf', filename)
    if match:
        year = int("20" + match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        return datetime(year, month, day)
    else:
        raise ValueError(f"Unable to extract date from filename: {filename}")





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
        #newest_entry = json_data["data"][13]
        newest_file_path = newest_entry["path"]
        
        if last_downloaded != newest_file_path:
            # Download the new file
            local_file_path = download_file_to_disk(BASE_URL + newest_file_path, DOWNLOAD_DIR)
            #local_file_path = download_file_to_disk('https://www.dnb.no/portalfront/nedlast/no/markets/analyser-rapporter/norske/anbefalte-aksjer/AA231009.pdf', DOWNLOAD_DIR)
            print(f"Downloaded {local_file_path}")

            report_date = extract_date_from_filename(BASE_URL + newest_file_path)

            # Remove restrictions from the file
            clean_local_file_path = remove_restrictions(local_file_path)

            # Upload file to GCS
            gcs_path = upload_to_gcs(clean_local_file_path, G_BUCKET_NAME,storage_client)
            print(f"Uploaded to GCS at {gcs_path}")

            # Extract text from the file
            text = extract_text_from_pdf(local_file_path)
            #print(f"Extracted Text: {text}")

            delete_files_from_directory(DOWNLOAD_DIR)

            df_ukens_aksjer = get_json_from_text(text)

            append_to_bigquery(df_ukens_aksjer, 'alt.dnb_ukens_aksjer_detalj', G_PRJ_ID)

            append_to_bigquery(create_dataframe(BASE_URL + newest_file_path, gcs_path, text, datetime.now(OSLO_TZ)), 'alt.dnb_ukens_aksjer', G_PRJ_ID)

            # Update the last downloaded path
            last_downloaded = newest_file_path

        # Sleep for the interval before checking again
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()



def manual_batch():
    # Fetch the JSON
    response = requests.get(JSON_URL)
    json_data = response.json()

    # Loop over all the entries in the JSON data
    for entry in json_data["data"]:
        file_path = entry["path"]

        # Download the file
        print(BASE_URL + file_path)
        local_file_path = download_file_to_disk(BASE_URL + file_path, DOWNLOAD_DIR)
        print(f"Downloaded {local_file_path}")

        report_date = extract_date_from_filename(BASE_URL + file_path)
        report_date = OSLO_TZ.localize(report_date)
        report_date = report_date.replace(hour=8, minute=0)

        # Remove restrictions from the file
        clean_local_file_path = remove_restrictions(local_file_path)

        # Upload file to GCS
        gcs_path = upload_to_gcs(clean_local_file_path, G_BUCKET_NAME, storage_client)
        print(f"Uploaded to GCS at {gcs_path}")

        # Extract text from the file
        text = extract_text_from_pdf(local_file_path)
        #print(f"Extracted Text: {text}")

        delete_files_from_directory(DOWNLOAD_DIR)

        df_aksjer, df_endring = get_json_from_text(text)

        if len(df_aksjer)>0:
            df_aksjer['date'] = report_date
            append_to_bigquery(df_aksjer, 'alt.dnb_ukens_aksjer_aksjer', G_PRJ_ID)

        if len(df_endring)>0:
            df_endring['date'] = report_date
            append_to_bigquery(df_endring, 'alt.dnb_ukens_aksjer_endring', G_PRJ_ID)

        append_to_bigquery(create_dataframe(BASE_URL + file_path, gcs_path, text, report_date), 'alt.dnb_ukens_aksjer', G_PRJ_ID)



