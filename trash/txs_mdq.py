import numpy as np
import pandas as pd
import pickle

import get_functions as gf

tk_id = '1.3.1236' # id de token MONEDAPAR
names_prefix = r'moneda-par' # prefijos de las cuentas de par
with open('data/input/names_mdq.txt','r') as f:
    names_mdq = pd.Series(f.read().splitlines()) # nombres de mdq
omit_accounts = ['1.2.150830','1.2.151476','1.2.667678'] # id cuenta del gob, propuesta y pamelaps
dia = '2018-09-11'

accounts = gf.get_accounts(prefix=names_prefix)
accounts_mdq = accounts.loc[accounts.name.isin(names_mdq)]
accounts_history = pickle.load(open("data/accounts_history_"+dia+".p", "rb"))
accounts_history_mdq = accounts_history[accounts_mdq.id_user]

txs_accounts_mdq = [gf.get_user_txs_fromhistory(json_account_history=i,token_id=tk_id) for i in accounts_history_mdq]

txs_mdq = pd.concat(txs_accounts_mdq).drop_duplicates().sort_values('datetime', ascending=True)
# merge con nombres de cuentas
txs = pd.merge(txs_mdq, accounts, how='left', left_on='sender_id', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns={'name':'sender_name'})
txs = pd.merge(txs, accounts, how='left', left_on='recipient_id', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns = {'name':'recipient_name'})
# omite txs en las que participan omit_accounts
txs = txs.loc[~(txs.sender_id.isin(omit_accounts) | txs.recipient_id.isin(omit_accounts)),:]
# regenera index
txs.index = range(len(txs.index))
# data['time'] = pd.to_datetime(data['time'])
# write to csv file
txs.to_csv('data/output/transacciones_mdq_'+dia+'.csv')
