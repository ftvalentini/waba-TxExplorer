#%%
import numpy as np
import pandas as pd
import datetime, os, sys

import par_functions as pf

#%%
print("\nUpdating nodos memberships")
# data (accounts sin nodo accounts - avales y txs reemplazando pamelaps por nodomardelplata)
nodos = pf.nodos_data()
accounts = pf.get_accounts()
accounts = accounts.loc[~accounts.name.isin(nodos.name)]
avales = pd.read_pickle('output/raw/avales_history_full.p')
avales = avales.replace('pamelaps','nodomardelplata')
txs = pd.read_pickle('output/raw/txs_history_full.p')
txs = txs.replace('pamelaps','nodomardelplata')

# inicializa dataframe (guarda dictionary como initial para hacer chequeo al final)
members_initial = dict.fromkeys(nodos.name)
members_temp = pd.DataFrame({'name':accounts.name,'nodo':None})

#%% criterio: ya listado
for i in nodos.name:
    fname = 'data/working/nodos/'+i+'.txt'
    if os.path.isfile(fname):
        with open(fname,'r') as f:
            nombres = f.read().splitlines()
        members_initial[i] = nombres
        members_temp.loc[members_temp.name.isin(nombres),'nodo'] = i
# va agregando a final y modificando temporal (initial con nombres originales):
members_final = members_temp.loc[members_temp.nodo.notnull()]
members_temp = members_temp.loc[members_temp.nodo.isnull()]

#%% criterio: recibio aval del nodo
avales_tab = avales.loc[avales.sender_name.isin(nodos.name) & ~avales.recipient_name.isin(pd.concat([nodos.name,members_final.name])),
                        ['sender_name','recipient_name']].drop_duplicates()
avales_tab.columns = ['nodo','name']
members_temp = pd.merge(members_temp.drop('nodo',axis=1), avales_tab, how='left', left_on='name', right_on='name')
# agrega al definitivo y saca del temporal:
members_final = members_final.append(members_temp.loc[members_temp.nodo.notnull()])
members_temp = members_temp.loc[members_temp.nodo.isnull()]

#%% criterio: recibio MONEDAPAR del nodo
txs_tab = txs.loc[txs.sender_name.isin(nodos.name) & ~txs.recipient_name.isin(pd.concat([nodos.name,members_final.name])),
                 ['sender_name','recipient_name']].drop_duplicates()
txs_tab.columns = ['nodo','name']
members_temp = pd.merge(members_temp.drop('nodo',axis=1), txs_tab, how='left', left_on='name', right_on='name')
# agrega al definitivo y saca del temporal:
members_final = members_final.append(members_temp.loc[members_temp.nodo.notnull()])
members_temp = members_temp.loc[members_temp.nodo.isnull()]

#%% criterio: recibio aval de user del nodo (avales sin omit accounts ni special tx)
avalesf = pf.filter_omitaccounts_avales(avales)
avalesf = pf.filter_special(avalesf)
avales_u_tab = avalesf.loc[avalesf.sender_name.isin(members_final.name) &
                          ~avalesf.recipient_name.isin(pd.concat([nodos.name,members_final.name])),
                          ['sender_name','recipient_name']].drop_duplicates()
avales_u_tab = pd.merge(avales_u_tab, members_final, how='left', left_on='sender_name', right_on='name').loc[:,['nodo','recipient_name']]
avales_u_tab.columns = ['nodo','name']
avales_u_tab = avales_u_tab.drop_duplicates()
# alerta si un user pertenece a mas de un nodo:
if len(avales_u_tab.name.unique())>len(avales_u_tab.name):
    sys.exit("User/s received avales from users of different nodes")
members_temp = pd.merge(members_temp.drop('nodo',axis=1), avales_u_tab, how='left', left_on='name', right_on='name')
# agrega al definitivo y saca del temporal:
members_final = members_final.append(members_temp.loc[members_temp.nodo.notnull()])
members_temp = members_temp.loc[members_temp.nodo.isnull()]

#%% criterio: recibio pares de user del nodo (txs sin omit accounts ni special)
txsf = pf.filter_omitaccounts_txs(txs)
txsf = pf.filter_special(txsf)
txs_u_tab = txsf.loc[txsf.sender_name.isin(members_final.name) &
                     ~txsf.recipient_name.isin(pd.concat([nodos.name,members_final.name])),
                    ['sender_name','recipient_name']].drop_duplicates()
txs_u_tab = pd.merge(txs_u_tab, members_final, how='left', left_on='sender_name', right_on='name').loc[:,['nodo','recipient_name']]
txs_u_tab.columns = ['nodo','name']
txs_u_tab = txs_u_tab.drop_duplicates()
# alerta si un user pertenece a mas de un nodo:
if len(txs_u_tab.name.unique())>len(txs_u_tab.name):
    sys.exit("User/s received PAR from users of different nodes")
members_temp = pd.merge(members_temp.drop('nodo',axis=1), txs_u_tab, how='left', left_on='name', right_on='name')
# agrega al definitivo y saca del temporal:
members_final = members_final.append(members_temp.loc[members_temp.nodo.notnull()])
members_temp = members_temp.loc[members_temp.nodo.isnull()]

#%% tabla final
members_final = members_final.append(members_temp).fillna('otros')
# save as csv
members_final.to_csv("output/final/nodo_nombre.csv",index=False)

#%% actualiza txt de nodos (solo si la nueva lista es mayor o igual que la vieja)
for i in nodos.name.tolist():
    nombres = members_final.loc[members_final.nodo==i,'name']
    old_nombres = members_initial[i]
    if old_nombres is not None:
        if len(nombres)<len(old_nombres):
            sys.exit(i+r": New members list is shorter than older list. txt is ahead of accounts data.")
    if old_nombres is not None:
        new_nombres = [x for x in nombres if x not in old_nombres]
    else:
        new_nombres = nombres
    with open('data/working/nodos/'+i+'.txt','a+') as f:
        for n in new_nombres:
            f.write(n +"\n")

#%%
print("Nodos memberships updated\n")
