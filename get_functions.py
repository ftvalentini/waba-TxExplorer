import numpy as np
import pandas as pd
import re, sys, urllib.request, json, pickle

def token_data():
    """
    Names and ids of MonedaPAR tokens
    """
    tokens = pd.DataFrame({'id':['1.2.1236','1.3.1237','1.3.1319','1.3.1320','1.3.1322'],
                           'name':['MONEDAPAR','DESCUBIERTOPAR','MONEDAPAR.AI','MONEDAPAR.AXXX','MONEDAPAR.AX']})
    return tokens

def get_accounts(prefix=r'moneda-par'):
    """
    Return DataFrame with name and id of users with prefix in name.
    CUIDADO: solo devuelve los primeros mil nombres
    """
    url = 'http://185.208.208.184:5000/lookup_accounts?start='+prefix
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    # devuelve nombres sin que el match sea total
    correct = pd.Series([i[0] for i in data]).str.contains(prefix)
    data_correct = np.array(data)[correct].tolist()
    data_series = [pd.Series(i) for i in data_correct]
    df = pd.DataFrame(data_series)
    df.columns = ['name','id_user']
    df['name'] = df['name'].str.replace(prefix+r'.','')
    return df

def get_user_history(user_id, max_page_num=1):
    """
    Return json-dictionary with last max_page_num*20 operations of user_id
    """
    data = []
    for page in range(max_page_num):
        url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id='+user_id+'&page='+str(page)
        response_temp = urllib.request.urlopen(url)
        data_temp = json.loads(response_temp.read())
        if len(data_temp)>0:
            data += data_temp
        else:
            break
    return data

def get_register(json_account_history):
    """
    Get register time of a json_account_history of a user
    """
    data_register = [i for i in json_account_history if i['op'][0]==5]
    register_time = data_register[0]['timestamp']
    return register_time

def convert_datetime(string, timezone='America/Buenos_Aires'):
    """
    Convert date string to pandas dataframe date with timezone.
    """
    string = '2017-08-01T15:33:06'
    datetime = pd.to_datetime(string).tz_localize('UTC').tz_convert(timezone)
    return datetime

def get_user_txs_fromhistory(json_account_history, timezone="America/Buenos_Aires"):
    """
    Get dataframe with data from a user's json_account_history where MonedaPAR tokens are involved.
    """
    data_tokens = [i for i in json_account_history if 'amount' in i['op'][1] and i['op'][1]['amount']['asset_id'] != '1.3.0']
    txs = []
    for txi in range(len(data_tokens)):
        tx_temp = data_tokens[txi]
        tx_data_temp = {
            'id': tx_temp['id'],
            'datetime': tx_temp['timestamp'],
            'amount': tx_temp['op'][1]['amount']['amount']/100,
            'asset_id': tx_temp['op'][1]['amount']['asset_id'],
            'sender_id': tx_temp['op'][1]['from'],
            'recipient_id': tx_temp['op'][1]['to']}
        txs.append(tx_data_temp)
    data = pd.DataFrame(txs)
    data['datetime'] = pd.to_datetime(data['datetime']).dt.tz_localize('UTC').dt.tz_convert(timezone)
    return data

def merge_txs_data(txs_df, accounts_df, tokens_df):
    """
    Merge dataframe of transactions from get_user_txs with data of tokens and users.
    """
    # merge con tokens
    tokens_df.columns = ['asset_id','asset_name']
    txs = pd.merge(txs_df, tokens_df, how='left', left_on='asset_id', right_on='asset_id')
    # merge con users
    admin_acc = pd.DataFrame({'name':['ADMIN','ADMIN'], 'id_user':['1.2.151476','1.2.150830']})
    accounts_df = accounts_df.append(admin_acc)
    txs = pd.merge(txs, accounts_df, how='left', left_on='sender_id', right_on='id_user').drop('id_user',axis=1)
    txs = txs.rename(columns={'name':'sender_name'})
    txs = pd.merge(txs, accounts_df, how='left', left_on='recipient_id', right_on='id_user').drop('id_user',axis=1)
    txs = txs.rename(columns = {'name':'recipient_name'})
    return txs

def get_user_name(user_id, account_prefix=r'moneda-par'):
    """
    Get user name from user id.
    """
    url = 'http://185.208.208.184:5000/account_name?account_id='+user_id
    response = urllib.request.urlopen(url)
    name = json.loads(response.read())
    return re.sub(account_prefix+r'.','',name)

def get_user_txs(user_id, token_id='1.3.1236', max_page_num=1):
    """
    Deprecated
    """

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
