# %%
import numpy as np
import pandas as pd
import re, sys, urllib.request, json
import par_functions as pf

# %% parametros
dia = "2018-11-03"
timezone = "America/Buenos_Aires"
hora_inicio = "05:00:00"
hora_fin = "23:00:00"
# accounts de todas las cuentas Par
accounts = pf.get_accounts_old(prefix='moneda-par')
# id del nodo
id_nodo = accounts.loc[accounts.name=='nodogualeguaychu','id_user'].tolist()[0]
# accounts a omitir
omit_accounts = ['1.2.150830','1.2.151476','1.2.152779',id_nodo] # gob, propuesta, tuti y nodo

# %% names de participantes
# history reciente de nodo
history_nodo = pf.get_user_history(user_id=id_nodo, max_page_num=4)
# dataframe de txs
txs_nodo = pf.get_par_txs_fromhistory(json_account_history=history_nodo)
# user_names que recibieron 300 Par de cuenta nodo (mas seba, mariocaf y larsen)
ids_feria_g = txs_nodo.loc[(txs_nodo.sender_id==id_nodo) & (txs_nodo.amount==300.0),'recipient_id'].unique().tolist()
ids_feria = ids_feria_g + accounts.loc[accounts.name.isin(['seba','mariocaf','larsen']),'id_user'].tolist()
names_feria = accounts.loc[accounts.id_user.isin(ids_feria),'name']

# with open('data/working/nodos/nodogualeguaychu.txt','w') as fi:
#     for n in names_feria.tolist():
#         fi.write(n +"\n")

# %% history de cada usuario (lee 40 tx max por usuario - 2*20)
history_user = [pf.get_user_history(user_id=x, max_page_num=2) for x in list(ids_feria)]

# %% dataframe de txs
txs_user = [pf.get_par_txs_fromhistory(json_account_history=i) for i in history_user]

# ### PARA AGREGAR USUARIOS NO LISTADOS INICIALMENTE
# add = pd.Series(['julieta-marini75'])
# ids_add = list(accounts.loc[accounts.name.isin(add)].id)
# history_add = [pf.get_user_history(user_id=x, max_page_num=4) for x in ids_add]
# txs_add = [pf.get_par_txs_fromhistory(json_account_history=i) for i in history_add]
# txs_user = txs_user + txs_add

# convierte en dataframe ordenado por fecha (duplicados??? deberia mantener duplicados)
txs = pd.concat(txs_user).sort_values('datetime', ascending=True)
# merge con nombres y ajusta datetime segun timezone
accounts.columns = ['name','id']
txs = pf.transf_txsdf(txs, accounts_df=accounts, tokens_df=pf.token_data())
# solo PAR
txs = txs.loc[txs.asset_name=="MONEDAPAR"]
# omite txs en las que participan omit_accounts
txs = txs.loc[~(txs.sender_id.isin(omit_accounts) | txs.recipient_id.isin(omit_accounts)),:]
# filtra por datetime
start = pd.to_datetime(dia +' T '+hora_inicio, format='%Y-%m-%d T %H:%M:%S').tz_localize(timezone)
end = pd.to_datetime(dia +' T '+hora_fin, format='%Y-%m-%d T %H:%M:%S').tz_localize(timezone)
txs_feria = txs.loc[(txs['datetime']>=start) & (txs['datetime']<=end)]

# %% chequeos (cada compra es una venta) y elimina duplicados
if txs_feria.drop_duplicates(keep=False).shape[0]==0:
    print("HAY CONSISTENCIA")
if txs_feria.drop_duplicates(keep=False).shape[0]>0:
    print("OJO QUE NO HAY CONSISTENCIA")
# elimina tx duplicadas
txs_feria = txs_feria.drop_duplicates()
# chequeo si hay algun participante no inlcuido en la lista original
part_efectivos = np.unique([list(txs_feria.sender_name)+list(txs_feria.recipient_name)])
part_nuevos = accounts.loc[~accounts.name.isin(names_feria) & accounts.name.isin(part_efectivos)].name
if len(part_nuevos)>0:
    print('participantes no incluidos en la lista original: '+', '.join(str(x) for x in list(part_nuevos)))
# participantes listados que no realizaron transacciones
part_noactivos = names_feria[~names_feria.isin(part_efectivos)]
if len(part_noactivos)>0:
    print('participantes listados sin actividad: '+', '.join(str(x) for x in list(part_noactivos)))

# %% STATS GLOBALES
out = {'n_participantes': len(part_efectivos),
       'n_transacciones': len(txs_feria.id),
       'valor_total': txs_feria.amount.sum()}
print(out)

# %% guarda resultados en un csv
pd.Series(out).to_csv(path="data/results_feria_bsas_"+dia+".csv")
