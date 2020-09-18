from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from io import BytesIO
import boto3
from botocore.exceptions import ClientError
import pickle
import os
import creds

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]

def google_creds() :
    s3 = boto3.resource("s3",
        aws_access_key_id = os.environ["access_key"],
        aws_secret_access_key = os.environ["secret_access_key"])

    try :
            
        bucket = s3.Bucket('requests-runahr')
        with BytesIO() as data :
            bucket.download_fileobj('file_gsheets.pkl', data)
            data.seek(0)
            creds = pickle.load(data)
        
        #body = obj.get()['Body'].read()
        #creds = pickle.load(body)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
        
            pick_obj = pickle.dumps(creds)
            s3.Object('requests-runahr', 'file_gsheets.pkl').put(Body = pick_obj)
                    
    except ClientError :
        print("File not found")
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
    
        pick_obj = pickle.dumps(creds)
        s3.Object('requests-runahr', 'file_gsheets.pkl').put(Body = pick_obj)

    return creds