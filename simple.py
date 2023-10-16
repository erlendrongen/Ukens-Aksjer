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
from functools import wraps
from pdfminer.high_level import extract_text

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



def extract_text_from_pdf(file_path):
    text = extract_text(file_path)
    return text


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

# Now, when you call make_openai_request, it will retry on `openai.error.OpenAIError` exceptions.
#response = make_openai_request(meldinger)


def get_json_from_text(text):
    # Crop the text right after the table with last weeks portfolio
    cropped_text = text.split("Figure 2", 1)[0]

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


    #completion = openai.ChatCompletion.create(model="gpt-4", messages = meldinger,request_timeout=60)
    completion = make_openai_request(meldinger)

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


    #completion2 = openai.ChatCompletion.create(model="gpt-4", messages = meldinger2,request_timeout=60)
    completion2 = make_openai_request(meldinger2)

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




# Download the files
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


def chat_gpt_on_text_files():
    # Path to your folder containing the text files
    folder_path = './data/txt/'

    df_aksjer_all = pd.DataFrame()
    df_endring_all = pd.DataFrame()

    # Loop over all files in the folder and 
    for filename in os.listdir(folder_path):
        # Check if the file has a .txt extension
        if filename.endswith('.txt'):
            with open(os.path.join(folder_path, filename), 'r', encoding='utf-8') as file:
                # Read the content of the file into a variable
                file_content = file.read()
                
                df_aksjer, df_endring = get_json_from_text(file_content)

                if len(df_endring) > 0:
                    df_endring_all = pd.concat([df_endring_all, df_endring], ignore_index=True)

                if len(df_aksjer) > 0:
                    df_aksjer_all = pd.concat([df_aksjer_all, df_aksjer], ignore_index=True)

    return df_aksjer_all, df_endring_all

#load the PDF files and store as TXT
#manual_batch_txt_files_only()

df_aksjer_all, df_endring_all = chat_gpt_on_text_files()

#run ChatGPT on text files to extract data 