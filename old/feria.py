# %%
import numpy as np
import pandas as pd
import re, sys, urllib.request, json
import get_functions as gf

# parametros (junto a los nombres en namesferia_[fecha].txt)
dia = "2018-11-03"
timezone = "America/Buenos_Aires"
hora_inicio = "05:00:00"
hora_fin = "22:00:00"
omit_accounts = ['1.2.150830','1.2.151476','1.2.667678','1.2.1095159'] # gob, propuesta, pamelaps y nodomardelplata

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

# %% history de cada usuario (lee 80 tx max por usuario - 4*20)
history_user = [gf.get_user_history(user_id=x, max_page_num=4) for x in list(accounts_feria.id_user)]

# %% dataframe de txs
txs_user = [gf.get_user_txs_fromhistory(json_account_history=i) for i in history_user]

# # ### PARA AGREGAR USUARIOS NO LISTADOS INICIALMENTE
# add = pd.Series(['asli2018'])
# ids_add = list(accounts.loc[accounts.name.isin(add)].id_user)
# history_add = [gf.get_user_history(user_id=x, max_page_num=4) for x in ids_add]
# txs_add = [gf.get_user_txs_fromhistory(json_account_history=i) for i in history_add]
# txs_user = txs_user + txs_add

# convierte en dataframe ordenado por fecha (duplicados??? deberia mantener duplicados)
txs = pd.concat(txs_user).sort_values('datetime', ascending=True)
# merge con nombres y ajusta datetime segun timezone
txs = gf.merge_txs_data(txs, accounts_df=accounts, tokens_df=gf.token_data())
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
pd.Series(out).to_csv(path="data/results_feria_"+dia+".csv")
