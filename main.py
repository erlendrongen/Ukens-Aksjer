#basics1
import os
import sys
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
from functools import wraps

#import pyperclip

#Google Cloud
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

# Prepare Google cloud tools

G_KEY = os.environ.get('G_KEY')
G_SCOPES = ast.literal_eval(os.environ.get('G_SCOPES')) #to make the string into an array
G_PRJ_ID = os.environ.get('G_PRJ_ID')
G_BUCKET_NAME = os.environ.get('G_BUCKET_NAME')

#setting up bigquery and storage client
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

#Telegram
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = str(-1001953889700)
telegram_url = "https://api.telegram.org/bot"+TELEGRAM_TOKEN+"/sendMessage?chat_id="+CHAT_ID+"&text="

#load keys from existing reports from database to avoid adding duplicates: (in production we would use a staging table and a merge to make it waterproof)
sql = f"""SELECT url FROM `quant-349811.alt.dnb_ukens_aksjer`"""
df_existing_data = pd.read_gbq(sql, credentials=credentials)



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
        print("waitng for "+str(delta_seconds)+" seconds")
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
    
    print("waitng for "+str(delta_seconds)+" seconds")

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
    s = s.replace('\n', '')
    
    matches = [match for match in re.finditer(r'[{}]', s)]
    
    if not matches:
        raise ValueError("No valid JSON structure found in the string")
    
    # Initialize counters for open and closed brackets
    open_brackets = 0
    close_brackets = 0
    
    # Initialize positions for the start and end of the valid JSON string
    start_pos = None
    end_pos = None
    
    for match in matches:
        if match.group() == '{':
            open_brackets += 1
            if start_pos is None:
                start_pos = match.start()
        else:
            close_brackets += 1
        
        if open_brackets == close_brackets:
            end_pos = match.end()
            break
    
    if start_pos is not None and end_pos is not None:
        return json.loads(s[start_pos:end_pos])
    else:
        raise ValueError("No valid JSON structure found in the string")
    
def retry_decorator(max_retries=3, delay=5, allowed_exceptions=(Exception,)):
    """
    Retry decorator.

    :param max_retries: Maximum number of retries. Default is 3.
    :param delay: Delay between retries in seconds. Default is 5 seconds.
    :param allowed_exceptions: Exceptions that trigger a retry. Default is general Exception.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0

            while retries < max_retries:
                try:
                    return func(*args, **kwargs)

                except allowed_exceptions as e:
                    retries += 1
                    if retries >= max_retries:
                        raise  # Re-raise the last exception if max retries exceeded
                    print(f"Error: {e}. Retrying ({retries}/{max_retries}) in {delay} seconds...")
                    time.sleep(delay)

        return wrapper
    return decorator

# Usage

@retry_decorator(max_retries=3, delay=5, allowed_exceptions=(openai.error.OpenAIError,))
def make_openai_request(meldinger):
    return openai.ChatCompletion.create(model="gpt-4", messages=meldinger, request_timeout=60)


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


    #completion = openai.ChatCompletion.create(model="gpt-4", messages = meldinger)
    completion = make_openai_request(meldinger)

    print(completion.choices[0].message.content)

    #s = completion.choices[0].message.content


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


    #completion2 = openai.ChatCompletion.create(model="gpt-4", messages = meldinger2)
    completion2 = make_openai_request(meldinger2)

    print(completion2.choices[0].message.content)


    try:
        json_obj_aksjer = clean_json_string(completion.choices[0].message.content)
        df_aksjer = pd.DataFrame(json_obj_aksjer['Aksjer'])
        
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
        
        json_obj_endring = clean_json_string(completion2.choices[0].message.content)
        df_endring = pd.DataFrame(json_obj_endring['Endringer_denne_uken'])
        
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


def generate_message(df, BASE_URL, newest_file_path, OSLO_TZ):
    # Start with the default message
    message = 'This weeks changes in DNB Markets recommended portfolio:\n'
    
    # If dataframe is empty
    if df.empty:
        message = 'No changes in this weeks recommended portfolio:\n'
    
    else:
        # Accumulate Buy and Sell messages
        buy_messages = []
        sell_messages = []
        
        for index, row in df.iterrows():
            Selskap = row['Selskap']
            Selskap = 'Company X, (hidden for legal reasons)'  #remove this for the real company
            Endring = row['Endring']
            
            if 'ut' in Endring:
                sell_messages.append(f"Sell {Selskap}")
            elif 'inn' in Endring:
                buy_messages.append(f"Buy {Selskap}")
        
        # Combine buy and sell messages into the main message
        message += "\n".join(sell_messages + buy_messages)
    message += f"\n\nHere is the link to the latest report \n{BASE_URL}{newest_file_path}\n"
    message += datetime.now(OSLO_TZ).strftime('%Y-%m-%d %H:%M:%S %Z%z')+"\n"
    
    return message


def main():
    # If it is not monday, wait til next monday 00:00 (oslo time) 
    wait_until_next_monday(OSLO_TZ)
    # wait until 09:00 (oslo time)
    wait_until_9_am(OSLO_TZ)

    # On the first run, just mark the most recent entry as the last known entry
    response = requests.get(JSON_URL)
    json_data = response.json()
    last_downloaded = json_data["data"][0]["path"]

    while True:
        now = datetime.now(OSLO_TZ)
        

        # End the script if it's past 19:00 in Oslo/Norway time
        #if now.hour >= 19:break
            

        # Fetch the JSON
        response = requests.get(JSON_URL)
        json_data = response.json()

        # If there's a new entry
        
        newest_entry = json_data["data"][0]
        #newest_entry = json_data["data"][13]
        newest_file_path = newest_entry["path"]

        
        if last_downloaded != newest_file_path or 1==1:
            # Download the file
            print(BASE_URL + newest_file_path)
            local_file_path = download_file_to_disk(BASE_URL + newest_file_path, DOWNLOAD_DIR)
            print(f"Downloaded {local_file_path}")

            # Extract the date from filename
            report_date = extract_date_from_filename(BASE_URL + newest_file_path)
            report_date = OSLO_TZ.localize(report_date)
            report_date = report_date.replace(hour=8, minute=0)

            # Remove any restrictions from the file
            clean_local_file_path = remove_restrictions(local_file_path)

            # Upload file to GCS
            gcs_path = upload_to_gcs(clean_local_file_path, G_BUCKET_NAME, storage_client)
            print(f"Uploaded to GCS at {gcs_path}")

            # Extract text from the file
            text = extract_text_from_pdf(local_file_path) 
            #print(f"Extracted Text: {text}")

            #delete files from tempfolder
            delete_files_from_directory(DOWNLOAD_DIR) 

            # extract data from text
            df_aksjer, df_endring = get_json_from_text(text)

            # get the filename to write txt's to disk
            #filename = local_file_path.split('/')[-1].rsplit('.', 1)[0] # Extract the filename "AA220912.pdf"
            #with open("./data/txt/"+filename+".txt", "w", encoding="utf-8") as file:
                # Write the content of the text variable to the file
            #    file.write(text)

            #df = df_endring




            #check if we already loaded report in our database
            if not BASE_URL + newest_file_path in df_existing_data['url'].values:
                #generate message
                message = generate_message(df_endring, BASE_URL, newest_file_path, OSLO_TZ)
                #send Telegram Message
                response = requests.get(telegram_url+message)


                #write results to database
                if len(df_aksjer)>0:
                    df_aksjer['date'] = report_date
                    append_to_bigquery(df_aksjer, 'alt.dnb_ukens_aksjer_aksjer', G_PRJ_ID)

                if len(df_endring)>0:
                    df_endring['date'] = report_date
                    append_to_bigquery(df_endring, 'alt.dnb_ukens_aksjer_endring', G_PRJ_ID)

                append_to_bigquery(create_dataframe(BASE_URL + newest_file_path, gcs_path, text, report_date), 'alt.dnb_ukens_aksjer', G_PRJ_ID)

                print("data stored in database, ending script")
                

            else:
                print("we have already that record in the database")    
            
            sys.exit("ending the script")
        # Sleep for the interval before checking again
        print("sleeping for "+str(CHECK_INTERVAL)+" seconds")
        time.sleep(CHECK_INTERVAL)






#Manual jobs


#might need updates
def manual_batch_full():
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

        # Extract the date from filename
        report_date = extract_date_from_filename(BASE_URL + file_path)
        report_date = OSLO_TZ.localize(report_date)
        report_date = report_date.replace(hour=8, minute=0)

        # Remove any restrictions from the file
        clean_local_file_path = remove_restrictions(local_file_path)

        # Upload file to GCS
        gcs_path = upload_to_gcs(clean_local_file_path, G_BUCKET_NAME, storage_client)
        print(f"Uploaded to GCS at {gcs_path}")

        # Extract text from the file
        text = extract_text_from_pdf(local_file_path) 
        #print(f"Extracted Text: {text}")

        #delete files from tempfolder
        delete_files_from_directory(DOWNLOAD_DIR) 

        # extract data from text
        df_aksjer, df_endring = get_json_from_text(text)

        # get the filename to write txt's to disk
        #filename = local_file_path.split('/')[-1].rsplit('.', 1)[0] # Extract the filename "AA220912.pdf"
        #with open("./data/txt/"+filename+".txt", "w", encoding="utf-8") as file:
            # Write the content of the text variable to the file
        #    file.write(text)

        
        #write results to database
        if len(df_aksjer)>0:
            df_aksjer['date'] = report_date
            append_to_bigquery(df_aksjer, 'alt.dnb_ukens_aksjer_aksjer', G_PRJ_ID)

        if len(df_endring)>0:
            df_endring['date'] = report_date
            append_to_bigquery(df_endring, 'alt.dnb_ukens_aksjer_endring', G_PRJ_ID)

        append_to_bigquery(create_dataframe(BASE_URL + file_path, gcs_path, text, report_date), 'alt.dnb_ukens_aksjer', G_PRJ_ID)


#might need updates
def manual_batch_txt_files_only():
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

        # Extract the date from filename
        report_date = extract_date_from_filename(BASE_URL + file_path)
        report_date = OSLO_TZ.localize(report_date)
        report_date = report_date.replace(hour=8, minute=0)

        # Remove any restrictions from the file
        clean_local_file_path = remove_restrictions(local_file_path)

        # Extract text from the file
        text = extract_text_from_pdf(local_file_path) 
        #print(f"Extracted Text: {text}")

        #delete files from tempfolder
        delete_files_from_directory(DOWNLOAD_DIR) 

        # extract data from text
        #df_aksjer, df_endring = get_json_from_text(text)

        # get the filename to write txt's to disk
        filename = local_file_path.split('/')[-1].rsplit('.', 1)[0] # Extract the filename "AA220912.pdf"
        with open("./data/txt/"+filename+".txt", "w", encoding="utf-8") as file:
            # Write the content of the text variable to the file
            file.write(text)


if __name__ == "__main__":
    main()





