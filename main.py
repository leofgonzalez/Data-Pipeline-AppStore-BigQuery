import functions_framework
import requests, time, json, hashlib, gzip
from authlib.jose import jwt
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import pandas as pd
import threading

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.auth import default
from io import BytesIO
from google.cloud import storage

def download_key_from_gcs(bucket_name, file_name, dest_path):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(dest_path)
    print(f"Downloaded {file_name} to {dest_path}")

def get_report_ids_by_name(response_data, report_names):
    report_ids = {}
    for report in response_data.get('data', []):
        report_name = report['attributes']['name']
        if report_name in report_names:
            report_ids[report_name] = report['id']
    return report_ids

def get_instances_ids_by_report(response_data):
    instances_ids = {}
    for instance in response_data.get('data', []):
        instance_id = instance['id']
        granularity = instance['attributes']['granularity']
        date = instance['attributes']['processingDate']
        instances_ids[instance_id] = {
            'granularity': granularity,
            'processingDate': date
        }
    return instances_ids

def get_segments_by_instance(instance_id):
    URL_SEGMENTS = f'https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/{instance_id}/segments'
    r = requests.get(URL_SEGMENTS, headers=HEAD)
    segments_response = r.json()
    segments = {}
    for segment in segments_response.get('data',[]):
        segment_id = segment['id']
        url = segment['attributes']['url']
        checksum = segment['attributes']['checksum']
        segments[segment_id] = {
            'url': url,
            'checksum':checksum
        }
    return segments

def download_extract_validate(url,checksum,temp_file_name):
    response = requests.get(url)  

    with open(temp_file_name,'wb') as file:
        file.write(response.content)  

    md5 = hashlib.md5()
    with open(temp_file_name,'rb') as f:
        while chunk := f.read(8192):
              md5.update(chunk)
    if md5.hexdigest() != checksum:
        raise ValueError(f"Checksum mismatch!")
    else:
        print("Checksum validated.")

    extracted_file_path = temp_file_name[:-3]               
    with gzip.open(temp_file_name, 'rb') as f_in:       
        with open(extracted_file_path, 'wb') as f_out:
            f_out.write(f_in.read())

    return extracted_file_path

def send_to_gbq(extracted_file_path,table_id,project_id):
    df = pd.read_csv(extracted_file_path, delimiter='\t')
    df.columns = ['created_at' if col == df.columns[0] else col for col in df.columns]  

    if df.empty:
        print(f"No data found in {extracted_file_path}")
        return False

    credentials, _ = default()
    to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=credentials, api_method="load_csv")

    return True

def complete_process(report_ids,type):

    # First loop, collecting instances ID per report IDs
    for report_name, report_id in report_ids.items():

        r = requests.get(f'https://api.appstoreconnect.apple.com/v1/analyticsReports/{report_id}/instances', headers=HEAD)

        instances_response = r.json()
        instances_ids = get_instances_ids_by_report(instances_response)

        # Second loop, collecting segment_lists by instances IDs
        for instance_id, instance_data in instances_ids.items():                               

            r = requests.get(f'https://api.appstoreconnect.apple.com/v1/analyticsReportInstances/{instance_id}/segments',headers=HEAD)
            segments_response = r.json()
            segments_list = get_segments_by_instance(instance_id)

            # Third loop, collecting download URLs and downloading, extracting and validating each part
            for segment_id, segment_data in segments_list.items():                            

                print(f"TYPE: {type}, Report Name: {report_name}, Report Id: {report_id}, Instance ID: {instance_id}, Instance Granularity: {instance_data['granularity']},"
                f"Instance Data: {instance_data['processingDate']}, Segment URL: {segment_id}, Segment URL: {segment_data['url']}, Segment Checksum: {segment_data['checksum']}")

                # Downloading, extracting and validating
                extracted_file_path = download_extract_validate(segment_data['url'], segment_data['checksum'], temp_file_name)
                print(f"Downloaded and extracted file to: {extracted_file_path}")

                # Sending to GBQ
                clean_report_name = report_name.replace(" ", "")                                                                 # Adjusts to guarantee the report name is prepared
                clean_processing_date = instance_data['processingDate'].replace("-", "")                                         # to be automatically grouped in BigQuery (oriented to "_", underlines)

                table_id = f"{dataset}.{type}_{clean_report_name}_{instance_data['granularity']}_{clean_processing_date}"              
                if send_to_gbq(extracted_file_path, table_id, project_id):                                                       
                    print(f"File {report_name}_{instance_data['granularity']}_{instance_data['processingDate']} successfully uploaded to BigQuery.\n")
                else:
                    print(f"Report {report_name}_{instance_data['granularity']}_{instance_data['processingDate']} not uploaded.\n")

@functions_framework.http
def start(request):
    threading.Thread(target=get_data).start()
    return {"status": "Started processing"}, 200

def get_data():

    # Global variables
    global HEAD, request_id_ongoing_report, request_id_one_time_snapshot, params_limit, temp_file_name, project_id, dataset

    # Variables that identify the Google Cloud Storage bucket
    BUCKET_NAME = 'XXXXXXXXXX'          # Name of the GCS bucket where the auth key is stored
    FILE_NAME = 'XXXXXXXXXX.p8'         # Name of the private key file stored in GCS
    TEMP_KEY_PATH = f'/tmp/{FILE_NAME}' # Temporary local path to store the key

    # Variables that identify the App Store Acount
    KEY_ID = "XXXXXXXXXX"                                      # Recurring access key ID
    ISSUER_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"         # Team ID for App Store Connect
    EXPIRATION_TIME = int(round(time.time() + (20.0 * 60.0)))  # Expiration timestamp (20 minutes from now)

    # Download the key from Google Cloud Storage
    download_key_from_gcs(BUCKET_NAME, FILE_NAME, TEMP_KEY_PATH)

    # Open the downloaded private key
    with open(TEMP_KEY_PATH, 'r') as f:
        PRIVATE_KEY = f.read()

    # Authorization header with JWT (JSON Web Token)
    header = {
        "alg": "ES256",   # Algorithm used for JWT
        "kid": KEY_ID,    # Key ID used in the JWT
        "typ": "JWT"      # Type of the token
    }
    payload = {
        "iss": ISSUER_ID,             # Issuer ID (team ID)
        "iat": round(time.time()),    # Issued at time (current timestamp)
        "exp": EXPIRATION_TIME,       # Expiration time of the token
        "aud": "appstoreconnect-v1"   # Audience of the token (App Store Connect API)
    }

    # Generate the JWT token using the private key
    token = jwt.encode(header, payload, PRIVATE_KEY)
    JWT = 'Bearer ' + token.decode()  # Format the token as a Bearer token for authorization

    HEAD = {'Authorization': JWT}     # Authorization header ready for use

    # Report IDs for ongoing and one-time snapshot reports
    request_id_ongoing_report = 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'                      # Relatório ONGOING GERADO DE UM A DOIS DIAS ANTES DO ONE-TIME-SNAPSHOT
    request_id_one_time_snapshot = 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'                   # ONE-TIME-SNAPSHOT GERADO NO DIA 03/07/2024 - 18:06

    # Base URLs for fetching reports using the report IDs
    URL_ONE_TIME_REPORT = f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{request_id_one_time_snapshot}/reports'
    URL_ONGOING_REPORTS = f'https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/{request_id_ongoing_report}/reports'

    # List of reports relevant to our business needs at the time
    reports = [
        # Essential reports
        "App Store Discovery and Engagement Detailed",
        "App Downloads Detailed",
        "App Install Performance",
        "App Store Installation and Deletion Detailed",
        # Additional reports
        "App Disk Space Usage",
        "Location Sessions",
        "App Crashes Expanded"
    ]

    # Temporary file name to store downloaded CSV data
    temp_file_name = "temp_download.csv.gz"
    
    # Fixed parameters defining the destination for the data in BigQuery
    project_id = "XXXXXXXXXXXXXXX"
    dataset = "AppStore"

    params_limit = {'limit': 200}     # Set limit for the number of records to fetch in each request

    # Fetching the one-time snapshot report data
    r = requests.get(URL_ONE_TIME_REPORT, headers=HEAD, params=params_limit)
    report_response = r.json()
    report_ids_one_time_snapshot = get_report_ids_by_name(report_response,reports)
    print(f'ONE TIME SNAPSHOT REPORTS: \n {report_ids_one_time_snapshot}')

    # Fetching ongoing report data
    r = requests.get(URL_ONGOING_REPORTS, headers=HEAD, params=params_limit)
    report_response = r.json()
    report_ids_ongoing = get_report_ids_by_name(report_response,reports)
    print(f'ONGOING REPORTS: \n {report_ids_ongoing} \n\n\n')
      
    print("Starting to colect the ONE_TIME_SNAPSHOT data:")
    complete_process(report_ids_one_time_snapshot,'SNAPSHOT')

    print("Starting to colect the ONGOING data:")
    complete_process(report_ids_ongoing,'ONGOING')

    return "Data collection completed successfully.", 200