#%%
import numpy as np
import pandas as pd
import re, sys, datetime, pickle

import par_functions as pf

#%%
print("\nMaking summary timeseries")
# data (propuesta raw y txs clean)
propuesta_history = pickle.load(open("output/raw/propuesta_history.p", "rb"))
txs = pd.read_csv('output/final/txs_history_clean.csv',index_col=False)
nodo_members = pd.read_csv("output/final/nodo_nombre.csv",index_col=False)

#%% proceso para generar y guardar todos los resumenes posibles
def save_resumen(frec, nodo):
    # summaries by period
    print("Getting register timeseries frec="+frec+" nodo="+nodo)
    register = pf.timeseries_register(propuesta_history, nododata_df=nodo_members, frec=frec, nodo=nodo)
    print("Getting activity timeseries frec="+frec+" nodo="+nodo)
    activity = pf.timeseries_activity(txs, frec=frec, nodo=nodo)
    print("Getting txs timeseries frec="+frec+" nodo="+nodo)
    tx = pf.timeseries_txs(txs, frec=frec, nodo=nodo)
    # combinar y save
    out = pd.concat([register, activity, tx], axis=1, sort=True).fillna(0)
    out.index.name = 'period'
    out.to_csv("output/final/resumen/"+frec+"/resumen_"+nodo+".csv")

#%% save todas las combinaciones posibles (sin pamelaps , con all + otros)
f_l = ['m', 'd']
n_l = pf.nodos_data().name
n_l = n_l.loc[n_l!='pamelaps'].tolist() + ['otros','all']
[save_resumen(frec=f, nodo=n) for f in f_l for n in n_l]

print("Summary timeseries done\n")
