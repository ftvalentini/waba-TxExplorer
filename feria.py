# %%
import numpy as np
import pandas as pd
import re, sys, urllib.request, json
import get_functions as gf

# parametros (junto a los nombres en namesferia_[fecha].txt)
dia = "2018-09-22"
timezone = "America/Buenos_Aires"
hora_inicio = "08:00:00"
hora_fin = "19:00:00"
omit_accounts = ['1.2.150830','1.2.667678'] # gob y pamelaps

# %%
# lee nombres input
with open('data/input/namesferia_'+dia+'.txt','r') as f:
    names_feria = pd.Series(f.read().splitlines())
# accounts de todas las cuentas Par
accounts = gf.get_accounts(prefix='moneda-par')
# STOP si no encuentra un nombre de la feria entre las cuentas par
wrong_names = names_feria[~names_feria.isin(accounts.name)]
if len(wrong_names) > 0:
     sys.exit('NON-EXISTENT ACCOUNT NAMES: '+', '.join(str(x) for x in list(wrong_names)))
# accounts de participantes
accounts_feria = accounts.loc[accounts.name.isin(names_feria)]

# %% transacciones de cada usuario (lee 80 tx max por usuario - 4*20)
txs_user = [gf.get_user_txs(user_id=x, max_page_num=4) for x in list(accounts_feria.id_user)]

# PARA AGREGAR USUARIOS NO LISTADOS INICIALMENTE
# add = pd.Series(['nantili','leonelmachado','pauloismael'])
# ids_add = list(accounts.loc[accounts.name.isin(add)].id_user)
# txs_add = [gf.get_user_txs(user_id=x, max_page_num=4) for x in ids_add]
# txs_user = txs_user + txs_aa

# %%
# convierte en dataframe ordenado por fecha (sin duplicados)
txs = pd.concat(txs_user).sort_values('time', ascending=True)
# omite txs en las que participan omit_accounts
txs = txs.loc[~(txs.sender.isin(omit_accounts) | txs.recipient.isin(omit_accounts)),:]
# merge con los nombres
txs = pd.merge(txs, accounts, how='left', left_on='sender', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns={'name':'sender_name'})
txs = pd.merge(txs, accounts, how='left', left_on='recipient', right_on='id_user').drop('id_user',axis=1)
txs = txs.rename(columns = {'name':'recipient_name'})

# filtra por datetime
start = pd.to_datetime(dia +' T '+hora_inicio, format='%Y-%m-%d T %H:%M:%S').tz_localize(timezone)
end = pd.to_datetime(dia +' T '+hora_fin, format='%Y-%m-%d T %H:%M:%S').tz_localize(timezone)
txs['time'] = pd.to_datetime(txs['time']).dt.tz_localize('UTC').dt.tz_convert(timezone)
txs_feria = txs.loc[(txs['time']>=start) & (txs['time']<=end)]
# chequea consistencia (cada compra es una venta) y elimina duplicados
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
# STATS GLOBALES
out = {'n_participantes': len(part_efectivos),
       'n_transacciones': len(txs_feria.id_tx),
       'valor_total': txs_feria.amount.sum()}
print(out)

# %% guarda resultados en un csv
pd.Series(out).to_csv(path="data/results_feria_"+dia+".csv")
