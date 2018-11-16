#%%
import numpy as np
import pandas as pd
import re, sys, datetime, pickle, time

import par_functions as pf

#%%
# data (propuesta raw y txs clean)
propuesta_history = pickle.load(open("output/raw/propuesta_history.p", "rb"))
txs = pd.read_csv('output/final/txs_history_clean.csv',index_col=False)
saldos = pd.read_pickle("data/working/saldos_history_users.p")
nodo_members = pd.read_csv("output/final/nodo_nombre.csv",index_col=False)

#%% genera y guarda todos los resumenes posibles
frec_list = ['m','d']
nodos_list = pf.nodos_data().name
nodos_list = nodos_list.loc[nodos_list!='pamelaps'].tolist() + ['otros','all']

for f in frec_list:
    print("\nTime series " + f)
    start = time.time()
    for n in nodos_list:
        t0 = time.time()
        print("\tfor " + n + " ...   ", end="", flush=True)
        register = pf.timeseries_register(propuesta_history, nododata_df=nodo_members, frec=f, nodo=n)
        activity = pf.timeseries_activity(txs, frec=f, nodo=n)
        tx = pf.timeseries_txs(txs, frec=f, nodo=n)
        circ = pf.timeseries_circ(saldos, nododata_df=nodo_members, frec=f, nodo=n)
        out = pd.concat([register, activity, tx, circ], axis=1, sort=True).fillna(0)
        out.index.name = 'period'
        out.to_csv("output/final/resumen/"+f+"/resumen_"+n+".csv")
        t1 = time.time()
        print("[DONE] in " + str(round(t1-t0,1)) + " seconds")
    end = time.time()
    print("DONE in " + str(round(end-start,1)) + " seconds")


# register = pf.timeseries_register(propuesta_history, nododata_df=nodo_members, frec=f, nodo=n)
# activity = pf.timeseries_activity(txs, frec=f, nodo=n)
# tx = pf.timeseries_txs(txs, frec=f, nodo=n)
# circ = pf.timeseries_circ(saldos, nododata_df=nodo_members, frec=f, nodo=n)
# circ.shape[0]==0


# def save_resumen(frec, nodo):
#     # summaries by period
#     print("Getting register timeseries frec="+frec+" nodo="+nodo)
#     register = pf.timeseries_register(propuesta_history, nododata_df=nodo_members, frec=frec, nodo=nodo)
#     print("Getting activity timeseries frec="+frec+" nodo="+nodo)
#     activity = pf.timeseries_activity(txs, frec=frec, nodo=nodo)
#     print("Getting txs timeseries frec="+frec+" nodo="+nodo)
#     tx = pf.timeseries_txs(txs, frec=frec, nodo=nodo)
#     # combinar y save
#     out = pd.concat([register, activity, tx], axis=1, sort=True).fillna(0)
#     out.index.name = 'period'
#     out.to_csv("output/final/resumen/"+frec+"/resumen_"+nodo+".csv")
#
# #%% save todas las combinaciones posibles (sin pamelaps , con all + otros)
# f_l = ['m', 'd']
# n_l = pf.nodos_data().name
# n_l = n_l.loc[n_l!='pamelaps'].tolist() + ['otros','all']
# [save_resumen(frec=f, nodo=n) for f in f_l for n in n_l]
