#%%
import numpy as np
import pandas as pd
import pickle, sys

import par_functions as pf

#%% datos
print("\nExtracting txs")
# cuentas y tokens de moneda-par
accounts = pf.get_accounts()
tokens = pf.token_data()

#%% txs
# historial de users
accounts_history = pickle.load(open("output/raw/accounts_history.p", "rb"))
# transacciones de MONEDAPAR dentro de cada historial
txs_accounts = [pf.get_par_txs_fromhistory(json_account_history=i) for i in accounts_history]
# historico de transacciones (DROP DUPLICATES)
txs = pd.concat(txs_accounts).drop_duplicates().sort_values('datetime', ascending=False)
# enriquece/transforma df
txsm = pf.transf_txsdf(txs, accounts_df=accounts, tokens_df=tokens)

#%% save as pickle (because is raw)
txsm.to_pickle("output/raw/txs_history_full.p")

print("txs extracted\n")
