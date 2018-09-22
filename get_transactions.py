import numpy as np
import pandas as pd
import re
import urllib.request, json

import get_functions as gf

tk_id = '1.3.1236' # id de token MONEDAPAR
names_prefix = r'moneda-par' # prefijos de las cuentas de par
omit_accounts = ['1.2.150830'] # id cuenta del gob

# cuentas (names y ids)
accounts = gf.get_accounts(prefix=names_prefix)
# historico de transacciones de token por usuario (usar max_page_num=1 para actualizar)
txs_accounts = [gf.get_user_txs(user_id=i, max_page_num=999999999, token_id=tk_id) for i in list(accounts.id_user)]
#pickle.dump(txs_accounts, open("temp_data/txs_accounts.p", "wb"))
# # para cargarlo sin correrlo
# import pickle
# txs_accounts = pickle.load(open("temp_data/txs_accounts.p", "rb"))
# historico de transacciones
txs = pd.concat(txs_accounts).drop_duplicates().sort_values('time', ascending=True)
# # los envios a gobierno-par no estan duplicados:
# pd.concat(txs_accounts).drop_duplicates(keep=False).sort_values('time', ascending=True)
# merge con nombres de cuentas
txs = pd.merge(txs, accounts, how='left', left_on='sender', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns={'name':'sender_name'})
txs = pd.merge(txs, accounts, how='left', left_on='recipient', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns = {'name':'recipient_name'})
# omite txs en las que participan omit_accounts
txs = txs.loc[~(txs.sender.isin(omit_accounts) | txs.recipient.isin(omit_accounts)),:]
# regenera index
txs.index = range(len(txs.index))
# data['time'] = pd.to_datetime(data['time'])
# write to json file
txs.to_json('data/transacciones.json', orient='index')
