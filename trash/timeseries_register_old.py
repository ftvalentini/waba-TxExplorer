import numpy as np
import pandas as pd
import re, urllib.request, json, datetime, pickle

import get_functions as gf

#%% parameters
dia = '2018-10-24'
# history de propuesta-par
propuesta_history = pickle.load(open("output/raw/propuesta_history_"+dia+".p", "rb"))

#%% clean data
# nombres moneda-par registrados en history
register_history = [i for i in propuesta_history if i['op'][0]==5 and 'moneda-par' in i['op'][1]['name']]
register_names = pd.Series([i['op'][1]['name'] for i in register_history]).str.replace(r'moneda-par.','')
# fecha de registro de cada usuario
dates = [i['timestamp'] for i in register_history]
register_data = pd.Series(pd.to_datetime(dates).tz_localize('UTC').tz_convert('America/Buenos_Aires'),
                          index=register_names)
# dataframe ordenado por datetime
reg_df = pd.DataFrame(register_data.values, columns=['datetime'])
reg_df['ym'] = reg_df.datetime.dt.to_period(freq='m')
reg_df['ymd'] = reg_df.datetime.dt.to_period(freq='d')

#%% process
# ym
# groupby + count
out_df_ym = reg_df[['ym','datetime']].groupby(['ym']).count()
out_ym = pd.Series(out_df_ym.datetime, index=out_df_ym.index)
# merge con ym y ymd que pueda faltar en la data original
all_ym = pd.PeriodIndex(start=out_ym.index[0], end=dia, freq='m')
all_ym_df = pd.Series(np.full_like(all_ym,0), index=all_ym, dtype='int64')
out_ym = out_ym.add(all_ym_df, fill_value=0).cumsum()
# ymd
# groupby + count
out_df_ymd = reg_df[['ymd','datetime']].groupby('ymd').count()
out_ymd = pd.Series(out_df_ymd.datetime, index=out_df_ymd.index)
# merge con ymd y ymdd que pueda faltar en la data original
all_ymd = pd.PeriodIndex(start=out_ymd.index[0], end=dia, freq='d')
all_ymd_df = pd.Series(np.full_like(all_ymd,0), index=all_ymd, dtype='int64')
out_ymd = out_ymd.add(all_ymd_df,fill_value=0).cumsum()

#%% save
# hoy = datetime.datetime.today().date()
pd.DataFrame({'registered_accounts':out_ym}).to_csv("output/register_ym_"+dia+".csv")
pd.DataFrame({'registered_accounts':out_ymd}).to_csv("output/register_ymd_"+dia+".csv")
