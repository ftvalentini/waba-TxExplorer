import numpy as np
import pandas as pd
import re
import urllib.request, json

def get_accounts(prefix=r'moneda-par'):
    url = 'http://185.208.208.184:5000/lookup_accounts?start='+prefix
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    # OJO devuelve primeros 1000 nombres
    # y devuelve nombres sin que el match sea total
    correct = pd.Series([i[0] for i in data]).str.contains(prefix)
    data_correct = np.array(data)[correct].tolist()
    data_series = [pd.Series(i) for i in data_correct]
    df = pd.DataFrame(data_series)
    df.columns = ['name','id_user']
    df['name'] = df['name'].str.replace(prefix+r'.','')
    return df

def get_user_txs(user_id, token_id='1.3.1236', max_page_num=1):
    data = []
    for page in range(max_page_num):
        url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id=account_id%3D'+user_id+'&page='+str(page)
        response_temp = urllib.request.urlopen(url)
        data_temp = json.loads(response_temp.read())
        if len(data_temp)>0:
            data += data_temp
        else:
            break
    data_alltokens = [i for i in data if 'amount' in i['op'][1]]
    data_token = [i for i in data_alltokens if i['op'][1]['amount']['asset_id'] == token_id]
    txs = []
    for txi in range(len(data_token)):
        tx_temp = data_token[txi]
        tx_data_temp = {
            'id_tx': tx_temp['id'],
            'time': tx_temp['timestamp'],
            'amount': tx_temp['op'][1]['amount']['amount']/100,
            'asset': tx_temp['op'][1]['amount']['asset_id'],
            'sender': tx_temp['op'][1]['from'],
            'recipient': tx_temp['op'][1]['to']}
        txs.append(tx_data_temp)
    data_final = pd.DataFrame(txs)
    return data_final

def get_user_name(user_id, account_prefix=r'moneda-par'):
    url = 'http://185.208.208.184:5000/account_name?account_id='+user_id
    response = urllib.request.urlopen(url)
    name = json.loads(response.read())
    return re.sub(account_prefix+r'.','',name)
