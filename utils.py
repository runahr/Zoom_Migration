import pygsheets
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
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

class Sheets() :
    con = pygsheets.authorize(service_account_file= "client_secret.json")
    def Insert(self, dat, s) :
        con = self.con
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        #sheet.clear(start = "A1")
        
        sheet.set_dataframe(dat, start = "A1", extend = True, nan = "")
    def Get_DF(self, s) :
        con = self.con
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        df = sheet.get_as_df()
        return df
    def update_cells(self, s, lista) :
        con = self.con
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        sheet.clear(start = "A1")
        sheet.update_col(1, lista)

class fechas(): 
    def __init__(self, inicio = "2018-01-01", tipo = "months"):
        """
        inicio: fecha en formato %Y-%m-%d. Este valor se tomara como referencia para el calculo del intervalo. DEFAULT = 2017-11-01
        Solo para extracciones completas
        """
        if tipo == "months": 
            delta = relativedelta(months =+ 1)
        if tipo == "weeks": 
            delta = relativedelta(weeks=+1)

        self.tipo = tipo
        date1 = datetime.strptime(str(inicio), "%Y-%m-%d")
        d = date1
        salida = []
        while d<=datetime.strptime(str(date.today()), "%Y-%m-%d"): 
            salida.append(d.strftime("%Y-%m-%d"))
            d += delta
        self.fechas = salida
        self.ends = self.__last_day(self.fechas)
        self.fechas = [(start, end) for start, end in zip(self.fechas, self.ends)]
    
    def __last_day(self, lista):
        end_list = []
        if self.tipo == "months": 
            for i in lista: 
                dia = monthrange(int(i[:4]),int(i[5:7]))
                end_list.append("{}-{}-{}".format(i[:4],i[5:7],dia[1]))
            return end_list
        if self.tipo == "weeks":
            delta = relativedelta(weeks=+1)
            for i in lista: 
                end_list.append(str(datetime.strptime(str(i), "%Y-%m-%d") + delta)[:10])
            return end_list