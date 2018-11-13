#%%
import numpy as np
import pandas as pd
import re, sys, datetime

import par_functions as pf

#%%
print("\nCleaning data")
# data
txs = pd.read_pickle('output/raw/txs_history_full.p')
avales = pd.read_pickle('output/raw/avales_history_full.p')
nodo_members = pd.read_csv("output/final/nodo_nombre.csv",index_col=False)

#%% clean txs - filtra y merge con nodo_members (saca nodo accounts por el momento) [drop dup por las dudas]
# (no incluye omit accounts (ver docstring), no incluye nodo accounts, filtra especiales)
txs = pf.filter_omitaccounts_txs(txs)
txs = pf.filter_special(txs)
txs = pf.filter_nodoaccounts_txs(txs)
txs = pf.merge_txs_nododata(txs, nodo_members).drop_duplicates()

#%% clean avales - filtra [drop dup por las dudas]
# (no incluye omit accounts, no incluye nodos como recipients, filtra especiales)
avales = pf.filter_omitaccounts_avales(avales)
avales = pf.filter_nodoaccounts_avales(avales)
avales = pf.filter_special(avales).drop_duplicates()

#%% save as csv
txs.to_csv('output/final/txs_history_clean.csv',index=False)
avales.to_csv('output/final/avales_history_clean.csv',index=False)

print("\nData cleaned")
