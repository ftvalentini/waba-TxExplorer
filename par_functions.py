import numpy as np
import pandas as pd
import re, sys, urllib.request, json, pickle, datetime

def token_data():
    """
    Names and ids of MonedaPAR tokens
    """
    tokens = pd.DataFrame({'id':['1.3.1236','1.3.1237','1.3.1319','1.3.1320','1.3.1322'],
                           'name':['MONEDAPAR','DESCUBIERTOPAR','MONEDAPAR.AI','MONEDAPAR.AXXX','MONEDAPAR.AX']})
    return tokens

def nodos_data(prefix=r'moneda-par.nodo'):
    """
    Names and ids of nodos accounts
    """
    url = 'http://185.208.208.184:5000/lookup_accounts?start='+prefix
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    # API devuelve 1000 nombres (match total en los primeros)
    names = pd.Series([i[0].replace(r'moneda-par.','') for i in data])
    ids = pd.Series([i[1] for i in data])
    correct_i = pd.Series([i[0] for i in data]).str.contains(prefix)
    df = pd.DataFrame({'name':names,'id':ids}).loc[correct_i]
    # modificaciones: saca mdq y escobar, agrega pamelaps
    df = df.loc[~df.name.isin(['nodomdq','nodoescobar'])]
    df = df.append({'name':'pamelaps','id':'1.2.667678'}, ignore_index=True)
    return df

def gob_accounts():
    """
    Data of accounts whose transactions are not legitimate for stats
    MODIFICAR PARA QUE QUEDE STORED
    """
    out = pd.DataFrame({'name':['gobierno-par','propuesta-par'],
                        'id':['1.2.150830','1.2.151476']})
    return out

def update_omit_accounts(names_list):
    """
    Add names_list (without prefix) to stored data of accounts whose transactions (before 28/10/2018)
    or mutual transactions (anytime) are not legitimate.
    """
    omit_names = ['moneda-par.'+i for i in names_list]
    omit_ids = [get_user_id(i) for i in omit_names]
    add_df = pd.DataFrame({'name':omit_names, 'id':omit_ids})
    add_df['name'] = add_df['name'].str.replace(r'moneda-par.','')
    old_df = pd.read_csv("data/omit_accounts.csv", index_col=None)
    new_df = pd.concat([old_df,add_df], sort=False).sort_values(by='name')
    new_df.drop_duplicates(inplace=True)
    new_df.to_csv('data/omit_accounts.csv', index=False)

def omit_accounts():
    """
    Get stored data of accounts whose transactions (before 28/10/2018) or mutual transactions (anytime)
    are not legitimate.
    """
    out = pd.read_csv("data/omit_accounts.csv", index_col=None)
    return out

def get_accounts():
    """
    Return DataFrame with name and id of 'moneda-par.' users (from propuesta_history.p)
    """
    # history of propuesta par
    propuesta_history = pickle.load(open("output/raw/propuesta_history.p", "rb"))
    # nombres moneda-par registrados en history
    register_history = [i for i in propuesta_history if i['op'][0]==5 and 'moneda-par' in i['op'][1]['name']]
    names = [i['op'][1]['name'].replace(r'moneda-par.','') for i in register_history]
    ids = [i['result'].translate(str.maketrans('','',r'[""]')).split(',')[1] for i in register_history]
    # de mas viejo a mas nuevo:
    names.reverse()
    ids.reverse()
    # por algun motivo hay ops duplicadas en register history!!! (mismo id, cambia 'virtual_op')
    df = pd.DataFrame({'id':ids,'name':names}).drop_duplicates().reset_index(drop=True)
    return df

def get_propuesta_history(user_id='1.2.151476'):
    old = pickle.load(open("output/raw/propuesta_history.p", "rb"))
    old_i = [x['id'] for x in old]
    new = []
    # lista que testa interseccion entre old y new (not intersect)
    intersect = []
    i = 0
    while not intersect:
        url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id='+user_id+'&page='+str(i)
        print('page '+str(i)+' ...   ', end="", flush=True)
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        print('[DONE]')
        new += data
        intersect = [x for x in new if x['id'] in old_i]
        i = i+1
    out = new[:-len(intersect)] + old
    return out

def get_user_history(user_id):
    old = pickle.load(open("output/raw/accounts_history.p", "rb"))
    # si user no tiene history guardado
    if user_id not in old.index:
        out = []
        for i in range(999999999999999999999):
            url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id='+user_id+'&page='+str(i)
            print('page '+str(i)+' ...   ', end="", flush=True)
            response = urllib.request.urlopen(url)
            data = json.loads(response.read())
            print('[DONE]')
            if len(data)>0:
                out += data
            else:
                break
    # si user ya tenia history guardado
    else:
        old = old[user_id]
        old_i = [x['id'] for x in old]
        new = []
        # lista que testa interseccion entre old y new por medio de id de la operacion
        intersect = []
        i = 0
        while not intersect:
            url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id='+user_id+'&page='+str(i)
            print('page '+str(i)+' ...   ', end="", flush=True)
            response = urllib.request.urlopen(url)
            data = json.loads(response.read())
            print('[DONE]')
            new += data
            intersect = [x for x in new if x['id'] in old_i]
            i = i+1
        out = new[:-len(intersect)] + old
    return out

def get_register(json_account_history):
    """
    Get register time of a json_account_history of a user
    """
    data_register = [i for i in json_account_history if i['op'][0]==5]
    register_time = data_register[0]['timestamp']
    return register_time

def convert_datetime(string, timezone='America/Buenos_Aires'):
    """
    Convert UTC date string to pandas dataframe date with timezone.
    """
    datetime = pd.to_datetime(string).tz_localize('UTC').tz_convert(timezone)
    return datetime

def get_par_txs_fromhistory(json_account_history):
    """
    Get dataframe with data from a user's json_account_history where MONEDAPAR tokens are involved.
    """
    data_tokens = [i for i in json_account_history if 'amount' in i['op'][1] and i['op'][1]['amount']['asset_id']=='1.3.1236']
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
    data = pd.DataFrame(txs).reset_index(drop=True)
    #
    return data

def get_avales_fromhistory(json_propuesta_history):
    """
    Get dataframe with transfers of avales recorded in json_propuesta_history
    """
    avales = [i for i in json_propuesta_history if i['op'][0]==0 and
              i['op'][1]['amount']['asset_id'] in ('1.3.1319','1.3.1320','1.3.1322') and
              i['op'][1]['fee']['asset_id'] != '1.3.0']
    txs = []
    for i in range(len(avales)):
        tx_temp = avales[i]
        name_temp = bytes.fromhex(tx_temp['op'][1]['memo']['message']).decode('latin-1').split(r':',1)[1]
        tx_data_temp = {
            'id': tx_temp['id'],
            'datetime': tx_temp['timestamp'],
            'amount': tx_temp['op'][1]['amount']['amount'],
            'asset_id': tx_temp['op'][1]['amount']['asset_id'],
            'sender_id': tx_temp['op'][1]['from'],
            'recipient_name': name_temp}
        txs.append(tx_data_temp)
    df = pd.DataFrame(txs).reset_index(drop=True)
    return df

def transf_txsdf(txs_df, accounts_df, tokens_df, timezone="America/Buenos_Aires"):
    """
    Transform dataframe of txs from get_user_txs_fromhistory:
    Merges with data of tokens and users, changes datetime and switch columns.
    """
    tokens_df.columns = ['asset_id','asset_name']
    accounts_df = accounts_df.append(gob_accounts(), ignore_index=True, sort=True)
    accounts_df.columns = ['user_id','user_name']
    # merge con tokens
    df = pd.merge(txs_df, tokens_df, how='left', left_on='asset_id', right_on='asset_id')
    # merge con users
    df = pd.merge(df, accounts_df, how='left', left_on='sender_id', right_on='user_id').drop('user_id',axis=1)
    df = df.rename(columns={'user_name':'sender_name'})
    df = pd.merge(df, accounts_df, how='left', left_on='recipient_id', right_on='user_id').drop('user_id',axis=1)
    df = df.rename(columns = {'user_name':'recipient_name'})
    # datetime Argentina
    df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize('UTC').dt.tz_convert(timezone)
    # orden de las columnas
    df = df[['id','amount','asset_name','asset_id','datetime','sender_name','recipient_name','sender_id','recipient_id']]
    return df

def transf_avalesdf(avales_df, accounts_df, tokens_df, timezone="America/Buenos_Aires"):
    """
    Transform dataframe of avales from get_avales_fromhistory:
    Merges with data of tokens and users, changes datetime and switch columns.
    """
    tokens_df.columns = ['asset_id','asset_name']
    accounts_df = accounts_df.append(gob_accounts(), ignore_index=True, sort=True)
    accounts_df.columns = ['user_id','user_name']
    # merge con tokens
    df = pd.merge(avales_df, tokens_df, how='left', left_on='asset_id', right_on='asset_id')
    # merge con users
    df = pd.merge(df, accounts_df, how='left', left_on='sender_id', right_on='user_id').drop('user_id',axis=1)
    df = df.rename(columns={'user_name':'sender_name'})
    df = pd.merge(df, accounts_df, how='left', left_on='recipient_name', right_on='user_name').drop('user_name',axis=1)
    df = df.rename(columns = {'user_id':'recipient_id'})
    # datetime Argentina
    df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize('UTC').dt.tz_convert(timezone)
    # orden de las columnas
    df = df[['id','amount','asset_name','asset_id','datetime','sender_name','recipient_name','sender_id','recipient_id']]
    return df

def filter_omitaccounts_txs(txs_df):
    """
    Remove txs of accounts whose transactions (before 28/10/2018 and amount<50) or mutual transactions (anytime)
    are not legitimate in a txs dataframe. Government accounts are also removed.
    (API can't read ID of names with dots --> filtering with name)
    """
    admin = gob_accounts()
    other = omit_accounts()
    out = txs_df.loc[~(txs_df.sender_name.isin(admin.name) | txs_df.recipient_name.isin(admin.name)),:]
    out = out.loc[~(out.sender_name.isin(other.name) & out.recipient_name.isin(other.name)),:]
    out = out.loc[~( ( (out.sender_name.isin(other.name)) | (out.recipient_name.isin(other.name)) ) &
            (out.datetime<'28-10-2018') & (out.amount<50)),:]
    return out

def filter_omitaccounts_avales(df):
    """
    Remove avales ops where omit and gob accounts are involved
    (API can't read ID of names with dots --> filtering with name)
    """
    admin = gob_accounts()
    other = omit_accounts()
    out = df.loc[~(df.sender_name.isin(admin.name) | df.recipient_name.isin(admin.name)),:]
    out = df.loc[~(df.sender_name.isin(other.name) | df.recipient_name.isin(other.name)),:]
    return out

def filter_nodoaccounts_txs(txs_df):
    """
    Remove txs where nodo accounts are involved
    """
    nodos = nodos_data()
    df = txs_df.loc[~(txs_df.sender_name.isin(nodos.name) | txs_df.recipient_name.isin(nodos.name)),:]
    return df

def filter_nodoaccounts_avales(df):
    """
    Remove avales ops where nodo accounts receive avales
    """
    nodos = nodos_data()
    df = df.loc[~(df.recipient_name.isin(nodos.name)),:]
    return df

def filter_special(df):
    """
    Remove special cases from a txs/avales dataframe (eg users with more than one account in the same tx)
    """
    repeated = [['leomachado','leonelmachado'],
                ['mariocaf','larsen']]
    out = df
    for i in repeated:
        out = out.loc[~(out.sender_name.isin(i) & out.recipient_name.isin(i)),:]
    return out

def merge_txs_nododata(txs_df, nododata_df):
    """
    Merge dataframe of txs with nodo data of each user.
    """
    df = txs_df
    # merge
    df = pd.merge(df, nododata_df, how='left', left_on='sender_name', right_on='name').drop('name',axis=1)
    df = df.rename(columns={'nodo':'sender_nodo'})
    df = pd.merge(df, nododata_df, how='left', left_on='recipient_name', right_on='name').drop('name',axis=1)
    df = df.rename(columns={'nodo':'recipient_nodo'})
    return df

def saldo_history_user(txs_df, user_name, frec):
    """
    Get history of MONEDAPAR sent, received and cumulative balance by user_name from a txs_df (the raw one).
    frec equals 'm' or 'd'
    """
    txs = txs_df
    # frec column
    txs.loc[:,frec] = txs.datetime.dt.to_period(freq=frec)
    # all periods
    last_day = datetime.datetime.now()
    all = pd.PeriodIndex(start=txs.datetime.min(), end=last_day, freq=frec)
    all_df = pd.Series(np.full_like(all,0), index=all)
    # value sent and received each period
    send = txs.loc[txs.sender_name==user_name].groupby(frec).sum().amount.add(all_df, fill_value=0)
    rec = txs.loc[txs.recipient_name==user_name].groupby(frec).sum().amount.add(all_df, fill_value=0)
    # cumulative balance
    saldo_cum = (rec - send).cumsum()
    df = pd.DataFrame({'sent':send,'received':rec,'balance_cum':saldo_cum})
    return df

def timeseries_register(json_propuesta_history, nododata_df, frec, nodo):
    """
    Extrae timeseries de stock de cuentas 'moneda-par' registradas al final de cada periodo. Excluye cuentas-nodo.
    Frecuencia dada por frec ('m' o 'd'). nodo = 'all' o nombre-nodo.
    """
    # fechas y nombres de registros
    registros = [i for i in json_propuesta_history if i['op'][0]==5 and 'moneda-par' in i['op'][1]['name']]
    dates = [i['timestamp'] for i in registros]
    nombres = pd.Series([i['op'][1]['name'] for i in registros]).str.replace(r'moneda-par.','')
    # dataframe (timezone BsAs, ordenado, merge con nodo, elimina nodo=None (cuentas nodo))
    reg_df = pd.DataFrame({'datetime':pd.to_datetime(dates).tz_localize('UTC').tz_convert('America/Buenos_Aires'),
                          'name':nombres}).sort_values('datetime')
    reg_df['m'] = reg_df.datetime.dt.to_period(freq='m')
    reg_df['d'] = reg_df.datetime.dt.to_period(freq='d')
    reg_df = pd.merge(reg_df, nododata_df, how='right', left_on='name', right_on='name').drop('name',axis=1)
    if nodo!='all':
        reg_df = reg_df.loc[reg_df.nodo==nodo].drop('nodo',axis=1)
    # groupby + count (usa parameter freq)
    out = reg_df[[frec,'datetime']].groupby([frec]).count()
    out = pd.Series(out.datetime, index=out.index)
    # merge con periods que puedan faltar en la data original
    last_day = json_propuesta_history[0]['timestamp']
    all = pd.PeriodIndex(start=dates[-1], end=last_day, freq=frec)
    all = pd.Series(np.full_like(all,0), index=all, dtype='int64')
    out = out.add(all, fill_value=0).cumsum()
    df = pd.DataFrame({'registered_accounts':out})
    return df

def timeseries_activity(clean_txs_df, frec, nodo):
    """
    Extrae timeseries de actividad de cuentas en cada periodo
    Frecuencia dada por frec ('m' o 'd'). nodo = 'all' o nombre-nodo.
    """
    txs = clean_txs_df
    # periods
    dates = pd.to_datetime(txs.datetime.tolist()).tz_localize('UTC').tz_convert('America/Buenos_Aires')
    period = dates.to_period(freq=frec)
    txs.loc[:,frec] = period.values
    # todos los periods (por si falta alguno)
    last_day = datetime.datetime.now()
    all = pd.PeriodIndex(start=txs.datetime.min(), end=last_day, freq=frec)
    # filter by nodo (tx es del nodo si tiene recipient y/o sender)
    if nodo=='all':
        nodo = txs.sender_nodo.append(txs.recipient_nodo).unique().tolist()
    else:
        nodo = [nodo]
        txs = txs.loc[txs.sender_nodo.isin(nodo) | txs.recipient_nodo.isin(nodo)]
    ### active users by period
    # para cada period, calcula unique de senders+recipientes
    # loop porque no se con sql :(
    active = []
    for t in all:
        ids_sen_temp = txs.loc[(txs.loc[:,frec]==t) & (txs.sender_nodo.isin(nodo)),'sender_id']
        ids_rec_temp = txs.loc[(txs.loc[:,frec]==t) & (txs.recipient_nodo.isin(nodo)),'recipient_id']
        ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True)
        n_unique_temp = len(ids_temp.unique())
        active.append(n_unique_temp)
    accounts_with_tx = pd.Series(active, index=all)
    ### acum active users by period
    ids_old = []
    new = []
    for t in all:
        ids_sen_temp = txs.loc[(txs.loc[:,frec]==t) & (txs.sender_nodo.isin(nodo)),'sender_id']
        ids_rec_temp = txs.loc[(txs.loc[:,frec]==t) & (txs.sender_nodo.isin(nodo)),'recipient_id']
        ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True).unique()
        ids_new = ids_temp[~pd.Series(ids_temp).isin(ids_old)]
        ids_old += ids_new.tolist()
        new.append(len(ids_new))
    active_accounts = pd.Series(new, index=all).cumsum()
    # salida
    out = pd.DataFrame({'active_accounts':active_accounts, 'accounts_with_tx':accounts_with_tx})
    return out

def timeseries_txs(clean_txs_df, frec, nodo):
    """
    Extrae timeseries de txs (value and number) en cada periodo
    Frecuencia dada por frec ('m' o 'd')
    """
    txs = clean_txs_df
    # periods
    dates = pd.to_datetime(txs.datetime.tolist()).tz_localize('UTC').tz_convert('America/Buenos_Aires')
    period = dates.to_period(freq=frec)
    txs.loc[:,frec] = period.values
    # todos los periods (por si falta alguno)
    last_day = datetime.datetime.now()
    all = pd.PeriodIndex(start=txs.datetime.min(), end=last_day, freq=frec)
    all_df = pd.Series(np.full_like(all,0), index=all, dtype='int64')
    # filter by nodo (tx es del nodo si tiene recipient y/o sender)
    if nodo!='all':
        txs = txs.loc[txs.sender_nodo.isin([nodo]) | txs.recipient_nodo.isin([nodo])]
    # number of txs by period
    n = txs.groupby(frec).count().amount.add(all_df, fill_value=0)
    n_cum = n.cumsum()
    # value of txs by period
    value = txs.groupby(frec).sum().amount.add(all_df, fill_value=0)
    value_cum = value.cumsum()
    # salida
    out = pd.DataFrame({'n_transactions':n, 'value_transactions':value,
                        'n_transactions_cum':n_cum, 'value_transactions_cum':value_cum})
    return out

def timeseries_circ(saldos_history, nododata_df, frec, nodo):
    """
    Extrae timeseries de circulante en cada periodo de saldos_history series
    Frecuencia dada por frec ('m' o 'd')
    """
    # saldos diarios de cada user (ya incluye todas las fechas posibles)
    saldos = saldos_history
    # filter by nodo (saldos de users del nodo + cuenta-nodo)
    if nodo!='all':
        users_nodo = nododata_df.loc[nododata_df.nodo==nodo].name.tolist()
        if nodo!='otros':
            saldos = saldos[users_nodo + [nodo]]
        else:
            saldos = saldos[users_nodo]
    # saldos apilados (una fila por usuario-fecha)
    saldos_all = pd.concat(saldos.tolist())
    # saldos positivos
    saldos_pos = saldos_all.loc[saldos_all.balance_cum>0]
    # suma de saldos positivos
    circulante = saldos_pos.groupby(saldos_pos.index).sum().loc[:,['balance_cum']]
    circulante.columns = ['circ']
    # si nodo no tiene transacciones usa indice del primer usuario y pone ceros:
    if circulante.shape[0]==0:
        circulante = pd.DataFrame({'circ':0}, index=saldos[0].index)
    # agrupa por mes si necesario:
    if frec=='m':
        circulante.loc[:,frec] = circulante.index.asfreq(freq=frec)
        circulante = circulante.groupby(frec).last()
    return circulante

def get_user_name(user_id, account_prefix=r'moneda-par'):
    """
    Get user name from user id.
    """
    url = 'http://185.208.208.184:5000/account_name?account_id='+user_id
    response = urllib.request.urlopen(url)
    name = json.loads(response.read())
    return re.sub(account_prefix+r'.','',name)

def get_user_id(user_name):
    """
    Get user id from user name.
    """
    url = 'http://185.208.208.184:5000/account_id?account_name='+user_name
    response = urllib.request.urlopen(url)
    id = json.loads(response.read())
    return id

def saldo_user_OLD(txs_df, user_name, date):
    """
    DEPRECATED
    Get saldo MONEDAPAR from txs_df of user_name at date.
    """
    txs = txs_df
    txsd = txs.loc[txs.datetime<=date]
    send = txsd.loc[txsd.sender_name==user_name].amount.sum()
    rec = txsd.loc[txsd.recipient_name==user_name].amount.sum()
    saldo = rec - send
    return saldo

def get_accounts_OLD(prefix=r'moneda-par'):
    """
    DEPRECATED
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

def get_user_history_OLD(user_id, max_page_num=1):
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

def filter_tokens(txs_df, tk_names):
    """
    Get txs where tk_names are involved in a txs dataframe
    """
    tokens = token_data()
    tk_ids = tokens.loc[tokens.name.isin(tk_names),"id"]
    df = txs_df.loc[txs_df.asset_id.isin(tk_ids)]
    return df

# def get_user_txs(user_id, token_id='1.3.1236', max_page_num=1):
#     """
#     Deprecated
#     """
#     data = []
#     for page in range(max_page_num):
#         url = 'http://185.208.208.184:5000/account_history_pager_elastic?account_id=account_id%3D'+user_id+'&page='+str(page)
#         response_temp = urllib.request.urlopen(url)
#         data_temp = json.loads(response_temp.read())
#         if len(data_temp)>0:
#             data += data_temp
#         else:
#             break
#     data_alltokens = [i for i in data if 'amount' in i['op'][1]]
#     data_token = [i for i in data_alltokens if i['op'][1]['amount']['asset_id'] == token_id]
#     txs = []
#     for txi in range(len(data_token)):
#         tx_temp = data_token[txi]
#         tx_data_temp = {
#             'id_tx': tx_temp['id'],
#             'time': tx_temp['timestamp'],
#             'amount': tx_temp['op'][1]['amount']['amount']/100,
#             'asset': tx_temp['op'][1]['amount']['asset_id'],
#             'sender': tx_temp['op'][1]['from'],
#             'recipient': tx_temp['op'][1]['to']}
#         txs.append(tx_data_temp)
#     data_final = pd.DataFrame(txs)
#     return data_final
