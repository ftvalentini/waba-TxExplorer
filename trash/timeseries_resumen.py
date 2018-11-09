import numpy as np
import pandas as pd
from pandas import ExcelWriter
import openpyxl

import get_functions as gf

#%% parameters
dia = '2018-10-24'

#%% read data
# monthly
activity_ym = pd.read_csv('output/activity_ym_'+dia+'.csv', index_col=0)
register_ym = pd.read_csv('output/register_ym_'+dia+'.csv', index_col=0)
txs_ym = pd.read_csv('output/txs_ym_'+dia+'.csv', index_col=0)
# daily
activity_ymd = pd.read_csv('output/activity_ymd_'+dia+'.csv', index_col=0)
register_ymd = pd.read_csv('output/register_ymd_'+dia+'.csv', index_col=0)
txs_ymd = pd.read_csv('output/txs_ymd_'+dia+'.csv', index_col=0)

#%% export to excel
out_ym = pd.concat([activity_ym, register_ym, txs_ym], axis=1, sort=True).fillna(0)
out_ymd = pd.concat([activity_ymd, register_ymd, txs_ymd], axis=1, sort=True).fillna(0)
book = ExcelWriter('output/PAR_timeseries.xlsx')
out_ym.to_excel(book, 'monthly')
out_ymd.to_excel(book, 'daily')
book.save()
