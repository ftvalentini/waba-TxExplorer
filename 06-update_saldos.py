#%%
import numpy as np
import pandas as pd
import time

import par_functions as pf

#%% data
txs = pd.read_pickle('output/raw/txs_history_full.p')
accounts = pf.get_accounts()
nombres = accounts.name.tolist()

#%% update saldo history of each user (incluye nodo-accounts)
print("\nUpdating saldos ...   ", end="", flush=True)
t0 = time.time()
saldos = pd.Series(np.full_like(nombres,None), index=nombres)
for n in nombres:
    saldos[n] = pf.saldo_history_user(txs_df=txs, user_name=n, frec='d')

#%% check: sum of saldos equals zero each period
# recursion no funciona porque "In Python, recursion is limited to 999 calls"
# def add_recursive(df_list):
#     if len(df_list)==1:
#         return df_list[0]
#     else:
#         return df_list[0] + add_recursive(df_list[1:])
def add_dfs(df_list):
    df = df_list[0]
    i = 1
    while i<len(df_list):
        df = df + df_list[i]
        i = i+1
    return df
balance_global = add_dfs(df_list=saldos.tolist()).balance_cum.astype(int)
if (balance_global!=0).any():
    sys.exit("Sum of balances not equal to 0 in " + str(balance_global.index[balance_global!=0][0]))

#%% save as pickle
saldos.to_pickle("data/working/saldos_history_users.p")
t1 = time.time()
print("DONE in "+ str(round(t1-t0,1)) + " seconds")
