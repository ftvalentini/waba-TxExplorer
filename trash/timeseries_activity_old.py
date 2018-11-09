import numpy as np
import pandas as pd
import re, urllib.request, json, datetime, pickle

import get_functions as gf

#%% parameters
dia = '2018-11-01'
# history de transacciones
txs_full = pickle.load(open("output/raw/accounts_txs_history_"+dia+".p", "rb"))

#%% filtros
# solo MONEDAPAR (obsoleto)
# txs_par = txs_full.loc[txs_full.asset_name=="MONEDAPAR"]
# omite txs en las que participan omit accounts (ver gf.filter_usersintx)
txs_parf = gf.filter_usersintx(txs_full)
# omite casos especiales
txs_parf = gf.filter_specialtx(txs_parf)
# merge con otra data y correcion de datetime
txs_parf = gf.merge_txs_data(txs_parf, accounts_df=gf.get_accounts(), tokens_df=gf.token_data())

#%% variables year-month y year-month-day
ym = txs_parf.datetime.dt.to_period(freq='m')
ymd = txs_parf.datetime.dt.to_period(freq='d')
txs_parf.loc[:,'ym'] = ym.values
txs_parf.loc[:,'ymd'] = ymd.values
# ym y ymd de todo el periodo (por si falta alguno)
all_ym = pd.PeriodIndex(start=txs_parf.datetime.min(), end=dia, freq='m')
all_ymd = pd.PeriodIndex(start=txs_parf.datetime.min(), end=dia, freq='d')

#%% active users by ym y ymd
# para cada ym y ymd, calcula unique de senders+recipientes
# lo hago loop porque no se con sql :(
# para ym
aa_ym = []
for t in all_ym:
    ids_sen_temp = txs_parf.loc[txs_parf.ym==t,'sender_id']
    ids_rec_temp = txs_parf.loc[txs_parf.ym==t,'recipient_id']
    ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True)
    n_unique_temp = len(ids_temp.unique())
    aa_ym.append(n_unique_temp)
active_accounts_ym = pd.Series(aa_ym, index=all_ym)
# para ymd
aa_ymd = []
for t in all_ymd:
    ids_sen_temp = txs_parf.loc[txs_parf.ymd==t,'sender_id']
    ids_rec_temp = txs_parf.loc[txs_parf.ymd==t,'recipient_id']
    ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True)
    n_unique_temp = len(ids_temp.unique())
    aa_ymd.append(n_unique_temp)
active_accounts_ymd = pd.Series(aa_ymd, index=all_ymd)


#%% NEW active users by ym and ymd
# ym
old_ym = []
new_ym = []
for t in all_ym:
    ids_sen_temp = txs_parf.loc[txs_parf.ym==t,'sender_id']
    ids_rec_temp = txs_parf.loc[txs_parf.ym==t,'recipient_id']
    ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True).unique()
    ids_new = ids_temp[~pd.Series(ids_temp).isin(old_ym)]
    old_ym += ids_new.tolist()
    new_ym.append(len(ids_new))
new_active_accounts_ym = pd.Series(new_ym, index=all_ym).cumsum()
# ymd
old_ymd = []
new_ymd = []
for t in all_ymd:
    ids_sen_temp = txs_parf.loc[txs_parf.ymd==t,'sender_id']
    ids_rec_temp = txs_parf.loc[txs_parf.ymd==t,'recipient_id']
    ids_temp = ids_sen_temp.append(ids_rec_temp, ignore_index=True).unique()
    ids_new = ids_temp[~pd.Series(ids_temp).isin(old_ymd)]
    old_ymd += ids_new.tolist()
    new_ymd.append(len(ids_new))
new_active_accounts_ymd = pd.Series(new_ymd, index=all_ymd).cumsum()

#%% export to csv
# guarda como csv con fecha del dia de la consulta
out_ym = pd.DataFrame({'active_accounts':new_active_accounts_ym, 'accounts_with_tx':active_accounts_ym})
out_ym.to_csv("output/activity_ym_"+dia+".csv")
out_ymd = pd.DataFrame({'active_accounts':new_active_accounts_ymd, 'accounts_with_tx':active_accounts_ymd})
out_ymd.to_csv("output/activity_ymd_"+dia+".csv")
