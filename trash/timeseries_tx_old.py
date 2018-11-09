import numpy as np
import pandas as pd
import re, urllib.request, json, datetime, pickle

import get_functions as gf

#%% parameters
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
# merge con otra data y correcion de datetime
txs_parf = gf.merge_txs_data(txs_parf, accounts_df=gf.get_accounts(), tokens_df=gf.token_data())

#%% variables year-month y year-month-day
ym = txs_parf.datetime.dt.to_period(freq='m')
ymd = txs_parf.datetime.dt.to_period(freq='d')
txs_parf.loc[:,'ym'] = ym.values
txs_parf.loc[:,'ymd'] = ymd.values
# ym y ymd de todo el periodo (por si falta alguno)
all_ym = pd.PeriodIndex(start=txs_parf.datetime.min(), end=dia, freq='m')
all_ym_df = pd.Series(np.full_like(all_ym,0), index=all_ym, dtype='int64')
all_ymd = pd.PeriodIndex(start=txs_parf.datetime.min(), end=dia, freq='d')
all_ymd_df = pd.Series(np.full_like(all_ymd,0), index=all_ymd, dtype='int64')

#%% number of txs (ym y ymd)
# ym
n_ym = txs_parf.groupby('ym').count().amount.add(all_ym_df, fill_value=0)
# ymd
n_ymd = txs_parf.groupby('ymd').count().amount.add(all_ymd_df, fill_value=0)

#%% value of txs (ym y ymd)
# ym
value_ym = txs_parf.groupby('ym').sum().amount.add(all_ym_df, fill_value=0)
# ymd
value_ymd = txs_parf.groupby('ymd').sum().amount.add(all_ymd_df, fill_value=0)

#%% export to csv
# guarda como csv con fecha del dia de la consulta
out_ym = pd.DataFrame({'n_transactions':n_ym, 'value_transactions':value_ym})
out_ym.to_csv("output/txs_ym_"+dia+".csv")
out_ymd = pd.DataFrame({'n_transactions':n_ymd, 'value_transactions':value_ymd})
out_ymd.to_csv("output/txs_ymd_"+dia+".csv")

# txs_parf.loc[txs_parf.ymd==pd.Period('2018-08-11',freq='d')].sort_values('amount',ascending=False)
