import os
import requests
import datetime
from urllib.parse import quote
from utils2 import google_creds
import pygsheets
import time
import creds

RECORDING_TYPES = ['chat_file', 'shared_screen_with_gallery_view', 'gallery_view', 'shared_screen_with_speaker_view']

def main() :
    session = requests.Session()
    header = {
        'Authorization': 'Bearer {}'.format(os.environ["zoom_jwt"])
    }
    query = {
        'page_size' : 300,
        }
    req = session.get("https://api.zoom.us/v2/users", headers = header, params = query)
    dat = req.json()
    #Save users in a list
    l_users = []
    d_users = {}
    for i in dat['users'] :
        l_users.append( i.get('id') )
        d_users[ i.get('id') ] = i

    #date range for extraction
    end = datetime.date.today()
    start = end - datetime.timedelta(days=2)

    #Get all recordings for all users starting from 'start', month by month
    l_records = []
    for user in l_users :
        meetings = []
        query = {
            'page_size' : 300,
            'from' : str(start),
            'to' : str(end)
            }
        req = session.get("https://api.zoom.us/v2/users/{}/recordings".format(user), headers = header, params = query)
        dat = req.json()
        meetings = meetings + dat['meetings']
        while dat['next_page_token'] != '' :
            aux_q = {
                'page_size' : 300,
                'from' : str(start),
                'to' : str(end),
                'next_page_token' : '{}'.format(dat['next_page_token'])
                }
            req = session.get("https://api.zoom.us/v2/users/{}/recordings".format(user), headers = header, params = aux_q)
            dat = req.json()
            meetings = meetings + dat['meetings']
                
        #Save the required info    
        for x in meetings :
            if bool(x) and 'recording_files' in x :
                for y in x['recording_files'] :
                    if y.get('recording_type') in RECORDING_TYPES :
                        aux_d = {}
                        aux_d['recording_id'] = y.get('id')
                        aux_d['meeting_uuid'] = x.get('uuid')
                        aux_d['start_time'] = x.get('start_time')
                        aux_d['topic'] = x.get('topic')
                        aux_d['user_id'] = user
                        aux_d['user_email'] = d_users[user]['email']
                        aux_d['first_name'] = d_users[user]['first_name']
                        aux_d['last_name'] = d_users[user]['last_name']
                        aux_d['recording_start'] = y.get('recording_start')
                        aux_d['recording_end'] = y.get('recording_end')
                        aux_d['file_type'] = y.get('file_type')
                        aux_d['file_size'] = y.get('file_size')
                        aux_d['recording_type'] = y.get('recording_type')
                        
                        if y.get('download_url') == None :
                            aux_d['download_url'] = y.get('download_url')
                        else :
                            aux_d['download_url'] = y.get('download_url') + "?access_token={}".format(os.environ["zoom_jwt"])
                        l_records.append(aux_d)
                        
        print("Got {} URLs".format(d_users[user]['first_name']))
        
    con = pygsheets.authorize(service_account_file= "client_secret.json")
    spdsheet = con.open_by_url(os.environ["gsheet_url"])
    sheet = spdsheet.worksheet_by_title('Recordings')

    ids_uploaded = sheet.get_col(1, include_tailing_empty=False)

    print("Total Recordings: ", len(l_records))

    #For each record, call the API for uploading to drive
    for rec in l_records :
        if rec['recording_id'] not in ids_uploaded :
            event = {
                'url':rec['download_url'],
                'name': '{} {}'.format(rec['recording_start'], rec['topic']),
                'file_type':rec['file_type'].lower(),
                'user_id':rec['user_id'],
                'user_name' : '{} {}'.format(rec['first_name'], rec['last_name'])
            }
            r = session.get("https://9ffy69ln51.execute-api.us-east-1.amazonaws.com/default/UploadDrive", params = event)
            if 'file_id' in r.json() :
                file_id = r.json()['file_id']
                rec['drive_url'] = "https://drive.google.com/file/d/{}/view".format(file_id)
            else :
                print (r.json())
    
                
            #upload to sheet
            sheet.append_table(values = list(rec.values()), start = 'A1', dimension = 'ROWS', overwrite = False)
            print("uploaded {} {} from {} {} of size {}".format(rec['recording_start'], rec['topic'], rec['first_name'], rec['last_name'], rec['file_size']) )
            time.sleep(30)
            

main() 