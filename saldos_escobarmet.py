#%%
import numpy as np
import pandas as pd
import time, datetime

import par_functions as pf

#%% data (escobarmet users - name y id)
accounts = pf.get_accounts()
with open("data/working/nodos/nodoescobarmet.txt","r") as f:
    nombres = pd.Series(f.read().splitlines())
# nodo_members = pd.read_csv("output/final/nodo_nombre.csv")
# nombres = nodo_members.loc[nodo_members.nodo=='nodoescobarmet'].name.tolist()
escobarmet = accounts.loc[accounts.name.isin(nombres)]

#%% full history of users
users_full_history = []
for i in range(len(escobarmet.id)):
    print('reading account '+ escobarmet.id.iloc[i] +' ('+str(i+1)+' of '+str(len(escobarmet.id))+')')
    t0 = time.time()
    history = pf.get_user_history(user_id=escobarmet.id.iloc[i])
    t1 = time.time()
    print('DONE in '+ str(round(t1-t0,1)) + ' seconds\n')
    users_full_history += [history]
# pandas series
users_full_history = pd.Series(users_full_history, index=escobarmet.name)

#%% DataFrame de transacciones
# transacciones de MONEDAPAR dentro de cada historial
txs_accounts = [pf.get_par_txs_fromhistory(json_account_history=i) for i in users_full_history]
# DataFrame sin duplicates
txs = pd.concat(txs_accounts).drop_duplicates()
# enriquece/transforma df
txsm = pf.transf_txsdf(txs, accounts_df=accounts, tokens_df=pf.token_data())
# filtro temporal
end = pd.Timestamp.today(tz='America/Buenos_Aires').replace(day=23, hour=23, minute=59, second=59)
start = end - pd.DateOffset(months=1)
txsf = txsm.loc[(txsm.datetime>start) & (txsm.datetime<=end)]
# columnas
txsf = txsf[['datetime','amount','sender_name','recipient_name']]

#%% detalle de cada user
detalles = pd.Series(np.full_like(nombres,None), index=nombres)
for n in nombres:
    txs_n = txsf.loc[(txsf.recipient_name==n) | (txsf.sender_name==n)]
    txs_n = txs_n.sort_values('datetime', ascending=False).reset_index(drop=True)
    detalles[n] = txs_n

#%% resumen de saldos
# txs sin nodo account
txsr = txsf.loc[~((txsf.sender_name=='nodoescobarmet') | (txsf.recipient_name=='nodoescobarmet'))]
# gasto/ingreso by user (intra-extra-total)
resumen = pd.DataFrame(columns=['usuario','gasto_intra', 'gasto_extra', 'gasto',
                                'ingreso_intra','ingreso_extra', 'ingreso',
                                'saldo_intra', 'saldo_extra', 'saldo'])
for i in range(len(nombres)):
    g = txsr.loc[txsr.sender_name==nombres[i]].amount.sum()
    g_in = txsr.loc[(txsr.sender_name==nombres[i]) & (txsr.recipient_name.isin(nombres))].amount.sum()
    g_ex = g - g_in
    y = txsr.loc[txsr.recipient_name==nombres[i]].amount.sum()
    y_in = txsr.loc[(txsr.recipient_name==nombres[i]) & (txsr.sender_name.isin(nombres))].amount.sum()
    y_ex = y - y_in
    sal = y - g
    sal_in = y_in - g_in
    sal_ex = y_ex - g_ex
    resumen.loc[i] = [nombres[i], g_in, g_ex, g, y_in,y_ex, y, sal_in, sal_ex, sal]

#%% save detalles + resumen as csv
for n in detalles.index:
    detalles[n].to_csv("output/final/txs_escobarmet/detalle_"+ n +".csv", index=False)
resumen.to_csv("output/final/txs_escobarmet/resumen.csv", index=False)
