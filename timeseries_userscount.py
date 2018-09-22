import numpy as np
import pandas as pd
import re, urllib.request, json, datetime, pickle

import get_functions as gf

# history de propuesta-par
propuesta_history = pickle.load(open("data/propuesta_history_2018-09-22.p", "rb"))
# nombres moneda-par registrados en history
register_history = [i for i in propuesta_history if i['op'][0]==5 and 'moneda-par' in i['op'][1]['name']]
register_names = pd.Series([i['op'][1]['name'] for i in register_history]).str.replace(r'moneda-par.','')
# fecha de registro de cada usuario
dates = [i['timestamp'] for i in register_history]
register_data = pd.Series(pd.to_datetime(dates).tz_localize('UTC').tz_convert('America/Buenos_Aires'),
                          index=register_names)

# dataframe ordenado por datetime
reg_df = pd.DataFrame(register_data.values, columns=['datetime']).sort_values('datetime', ascending=True)
reg_df['datetime'] = pd.to_datetime(reg_df['datetime'])
reg_df['date'] = reg_df['datetime'].dt.date
reg_df['ym'] = reg_df['datetime'].dt.to_period(freq='m')
# groupby mes-año y count
out_df = reg_df[['ym','datetime']].groupby(['ym']).count()
out = pd.Series(out_df.datetime.cumsum(), index=out_df.index)
# merge con algun mes-año que pueda faltar en la data original
mes_hoy = pd.Period(datetime.datetime.today().date(),freq='m')
all_ym = pd.PeriodIndex(start=out.index[0], end=mes_hoy, freq='m')
all_ym_df = pd.Series(np.full_like(all_ym,0), index=all_ym)
out = out.add(all_ym_df)

# guarda como csv con fecha del dia
hoy = datetime.datetime.today().date()
out.to_csv("data/accounts_series_"+str(hoy)+".csv")
