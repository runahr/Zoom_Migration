import os
import requests
import pandas as pd
from utils import Sheets, fechas
from urllib.parse import quote
import creds
import time

RECORDING_TYPES = ['chat_file', 'shared_screen_with_gallery_view', 'gallery_view', 'shared_screen_with_speaker_view']

def main() :

    start = "2017-01-01"

    #Call Zoom API for Users
    session = requests.Session()
    header = {
        'Authorization': 'Bearer {}'.format(os.environ["zoom_jwt"])
    }
    dates = fechas(start).fechas
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
        
    #Get all recordings for all users starting from 'start', month by month
    l_records = []
    for user in l_users :
        meetings = []
        for date_range in dates :
            query = {
                'page_size' : 300,
                'from' : date_range[0],
                'to' : date_range[1]
                }
            req = session.get("https://api.zoom.us/v2/users/{}/recordings".format(user), headers = header, params = query)
            dat = req.json()
            meetings = meetings + dat['meetings']
            while dat['next_page_token'] != '' :
                aux_q = {
                    'page_size' : 300,
                    'from' : date_range[0],
                    'to' : date_range[1],
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

    print("Total Recordings: ", len(l_records))

    #For each record, call the API for uploading to drive
    for rec in l_records :
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

        #Delete recording from Zoom cloud
        """
        if rec['meeting_uuid'].startswith('/') or '//' in rec['meeting_uuid'] :
            muuid = quote(quote(rec['meeting_uuid'], safe=''), safe='')
        else :
            mmuid = rec['meeting_uuid']
        r2 = session.delete("https://api.zoom.us/v2/meetings/{}/recordings/{}".format(mmuid, rec['recording_id']), headers = header)
        if r2.status_code == 204 :
            rec['zoom_deleted'] = True
        else :
            rec['zoom_deleted'] = False
            """
        print("uploaded {} {} from {} {} of size {}".format(rec['recording_start'], rec['topic'], rec['first_name'], rec['last_name'], rec['file_size']) )
        time.sleep(30)
        
    df = pd.json_normalize(l_records)
    Sheets().Insert(df, 'Recordings')
    