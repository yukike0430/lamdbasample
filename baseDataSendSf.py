# -*- coding: utf-8 -*-
import requests
from mysql.connector import (connection)
import json
import sys
import os
from datetime import datetime, timedelta, timezone


#outh_token で接続し、tokenとinstanceURLを返します
def outh_token2(mode='login'):
    url = 'https://' + mode + '.salesforce.com/services/oauth2/token'
    print(url)
    payload = {
        'client_id':os.environ[mode + '_client_id'],
        'client_secret':os.environ[mode + '_client_secret'],
        'username':os.environ[mode + '_sfusername'],
        'password':os.environ['sfpassword'],
        'grant_type':'password',
        }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, params=payload, headers=headers)
    reqestjson = response.json()
    return reqestjson

#outh_token で接続し、tokenとinstanceURLを返します
def outh_token():
    url = 'https://ap.salesforce.com/services/oauth2/token'
    payload = {
        'client_id':os.environ['client_id'],
        'client_secret':os.environ['client_secret'],
        'username':os.environ['sfusername'],
        'password':os.environ['sfpassword'],
        'grant_type':'password'
        }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, params=payload, headers=headers)
    reqestjson = response.json()
    return reqestjson

# salesforceにデータを飛ばします
def send_data(instance_url,token,sfparam,sfdata):
    data = {'Authorization':'Bearer ' + token}
    url = instance_url + '/services/apexrest/ProductConnect/'
    response = requests.post(url ,headers=data,params=sfparam,data =json.dumps(sfdata,indent=4))
    return response

# SQLを実行し、結果を返します
def find_sql(query, paramcount):
    cnx = connection.MySQLConnection(
        user = os.environ['user'],
        password = os.environ['password'],
        host = os.environ['host'],
        database= os.environ['database']
    )
    data = list()

    timeparam = os.environ['minutesage']
    JST = timezone(timedelta(hours=+9), 'JST')
    now = datetime.now(JST)
    print(now)
    now1 = now - timedelta(minutes=int(timeparam))
    now2 = now - timedelta(minutes=int(timeparam))
    now3 = now1,now2
    print(now1)

    params = list()
    params.append(now)

    try:
        cur = cnx.cursor(buffered=True,dictionary=True)
        if paramcount == 0:
            cur.execute(query)
        elif paramcount == 1:
            cur.execute(query,(now1,))
        elif paramcount == 2:
            cur.execute(query, (str(now1), str(now2)))
        elif paramcount == 8:
            now3 = now1,now1,now1,now1,now1,now1,now1,now1
            cur.execute(query,(now3))

        for row in cur:
            tmp = []
            table_map = {}
            for key in row.keys():
                if key.find('.') > -1 :
                    table = key.split('.')[0]
                    col = key.split('.')[1]
                    if row[key] is not None:
                        table_map[table] = {col:row[key]}
                    tmp.append(key)

                elif row[key] is None or row[key] is '':
                    tmp.append(key)

                
                row[key] = escapeString(row[key])

            for tkey in tmp:
                row.pop(tkey)

            for table in table_map.keys():
                row[table] = table_map[table]

            data.append(row)
    except Exception as e:
        print(e)
    finally:
        cur.close()
        cnx.close()
    return data


# SQLを実行し、結果を返します
def find_sql_dict(query,paramcount):
    cnx = connection.MySQLConnection(
        host=os.environ['host'],
        port=os.environ['port'],
        user=os.environ['user'],
        password=os.environ['password'],
        database=os.environ['database']
    )

    timeparam = os.environ['minutesage']
    JST = timezone(timedelta(hours=+9), 'JST')
    now = datetime.now(JST)
    now1 = now - timedelta(minutes=int(timeparam))
    now2 = now - timedelta(minutes=int(timeparam))
    now3 = now1,now2

    try:
        cur = cnx.cursor(buffered=True,dictionary=True)
        if paramcount == 0:
            cur.execute(query)
        elif paramcount == 1:
            cur.execute(query,(now1,))
        elif paramcount == 2:
            cur.execute(query, (str(now1), str(now2)))
        elif paramcount == 8:
            now3 = now1,now1,now1,now1,now1,now1,now1,now1
            cur.execute(query,(now3))

        rows = cur.fetchall()
        
    except Exception as e:
        print(e)
    finally:
        cur.close()
        cnx.close()
    return rows

def find_sql_rownum_dict(query,paramcount):
    cnx = connection.MySQLConnection(
        host=os.environ['host'],
        port=os.environ['port'],
        user=os.environ['user'],
        password=os.environ['password'],
        database=os.environ['database']
    )

    timeparam = os.environ['minutesage']
    JST = timezone(timedelta(hours=+9), 'JST')
    now = datetime.now(JST)
    now1 = now - timedelta(minutes=int(timeparam))
    now2 = now - timedelta(minutes=int(timeparam))
    now3 = now1,now2
    try:
        cur = cnx.cursor(buffered=True,dictionary=True)
        if paramcount == 0:
            cur.execute(query)
            cur.execute(query)
        elif paramcount == 1:
            cur.execute(query, (now1,))
            cur.execute(query,(now1,))
        elif paramcount == 2:
            cur.execute(query, (str(now1), str(now2)))
            cur.execute(query, (str(now1), str(now2)))
        elif paramcount == 8:
            now3 = now1,now1,now1,now1,now1,now1,now1,now1
            cur.execute(query, (now3))
            cur.execute(query,(now3))

        rows = cur.fetchall()
        
    except Exception as e:
        print(e)
    finally:
        cur.close()
        cnx.close()
    return rows

# これで足りるかはわからないが、文字列だったらエスケープします。
def escapeString(text):
    if isinstance(text, str):
        return text.replace(u'\u3000',u' ') \
                   .replace(u'\n',u'\\n')
    else:
        return text

    # SQLを実行し、結果を返します
def find_sql_param(query,paramcount,param):
    cnx = connection.MySQLConnection(
        host=os.environ['host'],
        port=os.environ['port'],
        user=os.environ['user'],
        password=os.environ['password'],
        database=os.environ['database']
    )
    data = list()
    try:
        cur = cnx.cursor(buffered=True,dictionary=True)
        if paramcount == 0:
            cur.execute(query)
        elif paramcount == 1:
            cur.execute(query,(param,))
            
        for row in cur:
            for key in row.keys():
                if key.find('.') > -1 :
                    table = key.split('.')[0]
                    col = key.split('.')[1]
                    row[table] = {col:row[key]}
                    row.pop(key)
            data.append(row)
    except Exception as e:
        print(e)
    finally:
        cur.close()
        cnx.close()
    return data