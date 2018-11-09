import numpy as np
import pandas as pd
import pickle

import get_functions as gf

# CORREGIR SERIES DE TIEMPO!!!!!
    # HACER AJUSTE espurio
    # SACAR LOS FILTER MONEDAPAR QUE ESTAN AL PEDO


#%% transacciones de un mes de MDQ
dia = '2018-11-01'
# history de transacciones
txs_full = pickle.load(open("output/raw/accounts_txs_history_"+dia+".p", "rb"))
# nombres relevantes
coord_names = ['nodomardelplata','pamelaps']
omit_names = gf.omit_accounts().name.tolist()
gob_names = gf.gob_accounts().name.tolist()
# nombres segun si recibieron (300) par de cuentas coordinadoras
temp = txs_full.loc[txs_full.sender_name.isin(coord_names) & ~txs_full.recipient_name.isin(omit_names+gob_names),:]
users_names = temp.recipient_name.drop_duplicates()
# nombres segun las ferias
with open('data/input/names_mdq.txt','r') as t:
    names_ferias = pd.Series(t.read().splitlines())
# cuentas que recibieron 300 pero no estan en ferias
users_names.loc[~users_names.isin(names_ferias)]
users_names.loc[~users_names.isin(names_ferias)].shape
# cuentas que estan en ferias y no recibieron 300
names_ferias.loc[~names_ferias.isin(users_names)]
users_names.loc[~users_names.isin(names_ferias)].shape
# nombres totales
names_tot = pd.Series(names_ferias.tolist() + users_names.tolist()).unique()
# filtros
# solo MONEDAPAR
txs = txs_full.loc[txs_full.asset_name=="MONEDAPAR"]
# solo txs con nombres de mdq en AMBOS lados
txs = txs.loc[txs.sender_name.isin(names_tot) & txs.recipient_name.isin(names_tot)]
# omite casos especiales
txs = gf.filter_specialtx(txs)
# filtro temporal
txs = txs.loc[(txs['datetime']>='2018-10-01') & (txs['datetime']<='2018-10-31')]
# save csv
txs.to_csv("trash/txs_mdq-2018-10.csv")
# stats
out = {'n_participantes': len(np.unique([list(txs.sender_name)+list(txs.recipient_name)])),
       'n_transacciones': len(txs.id),
       'valor_total': txs.amount.sum()}
print(out)
# temp.recipient_name.shape
# temp.recipient_name.drop_duplicates().shape
# dup = temp.recipient_name.loc[temp.recipient_name.duplicated()].tolist()
# temp.loc[temp.recipient_name.isin(dup)]
# paulo = txs_full.loc[txs_full.sender_name.isin(['pauloismael']) & ~txs_full.recipient_name.isin(omit_names+gob_names),:]
# paulo.recipient_name
# avales = ['DESCUBIERTOPAR','MONEDAPAR.AI','MONEDAPAR.AXXX','MONEDAPAR.AX']
# ava = txs_full.loc[txs_full.sender_name.isin(names_mdq) | txs_full.recipient_name.isin(names_mdq),:]
# ava2 = ava.loc[ava.asset_name.isin(avales),:]


#%% update lista de omit_accounts
new = ['nodobuenosaires','nodomardelplata','franv0']
gf.update_omit_accounts(names_list=new)

#%% todos los usuarios de mdq
with open('data/input/namesferia_2018-10-20.txt','r') as f:
    names1 = f.read().splitlines()
with open('data/input/namesferia_2018-11-01.txt','r') as f:
    names2 = f.read().splitlines()
with open('data/input/names_mdq.txt','r') as f:
    namesm = f.read().splitlines()
allnames = np.unique(names1+names2+namesm).tolist()
with open('data/input/names_mdq.txt','w') as fi:
    for n in allnames:
        fi.write(n +"\n")

#%% datos de pamelaps
accounts = gf.get_accounts(prefix='moneda-par')
accounts_history = pickle.load(open("output/raw/accounts_history_2018-10-08.p", "rb"))
id_x = accounts.loc[accounts.name=='pamelaps','id_user']
history_x = accounts_history[id_x][0]
txs_x = gf.get_user_txs_fromhistory(history_x)
txs_m = gf.merge_txs_data(txs_x, accounts, gf.token_data())
txs_mf = gf.filter_tokens(txs_m, ['MONEDAPAR','MONEDAPAR.AI','MONEDAPAR.AXXX','MONEDAPAR.AX'])
txs_mf.to_csv('output/txs_pamelaps__2018-10-08.csv')

#%% peso de cuentas espurias
dia = '2018-10-24'
# history de transacciones
txs_full = pickle.load(open("output/raw/accounts_txs_history_"+dia+".p", "rb"))

#%% filtros
# solo MONEDAPAR
txs_par = txs_full.loc[txs_full.asset_name=="MONEDAPAR"]
# omite txs en las que participan omit accounts (ver gf.filter_usersintx)
txs_parf = gf.filter_usersintx(txs_par)
# omite casos especiales
txs_parf = gf.filter_specialtx(txs_parf)

omit_all = gf.omit_accounts()
omit = omit_all.loc[~omit_all.name.isin(['nodomardelplata','nodobuenosaires','pamelaps'])]

tot = txs_parf.amount.sum()
espurio_send = txs_parf.loc[txs_parf.sender_name.isin(omit.name)].amount.sum()
espurio_rec = txs_parf.loc[txs_parf.recipient_name.isin(omit.name)].amount.sum()
(espurio_rec+espurio_send)/tot*100

tot = txs_parf.amount.count()
espurio_send = txs_parf.loc[txs_parf.sender_name.isin(omit.name)].amount.count()
espurio_rec = txs_parf.loc[txs_parf.recipient_name.isin(omit.name)].amount.count()
(espurio_rec+espurio_send)/tot*100

tot = txs_parf.amount.count()
espurio_send = txs_parf.loc[txs_parf.sender_name==('seba')].amount.count()
espurio_rec = txs_parf.loc[txs_parf.recipient_name==('seba')].amount.count()
(espurio_rec+espurio_send)/tot*100

tot = txs_parf.amount.sum()
espurio_send = txs_parf.loc[txs_parf.sender_name==('seba')].amount.sum()
espurio_rec = txs_parf.loc[txs_parf.recipient_name==('seba')].amount.sum()
(espurio_rec+espurio_send)/tot*100


# # transactions df de propuesta
# propuesta_tx = gf.get_user_txs_fromhistory(propuesta_history)
# # merge con data tokens y accounts (y corrige timezone)
# txm = gf.merge_txs_data(propuesta_tx, accounts_df=acc, tokens_df=tok)
# txm.asset_name.unique()
# # filter: propuesta es sender y solo avales
# txmf = txm.loc[(txm.sender_name=='propuesta-par') & (txm.asset_name.isin(['MONEDAPAR.AI','MONEDAPAR.AX','MONEDAPAR.AXXX']))]


op_types = {'register': 5,
            'tx': 0,
            'whitelist/blacklist': 7,
            'issue': 14}
