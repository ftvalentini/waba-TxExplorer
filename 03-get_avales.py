#%%
import numpy as np
import pandas as pd
import re, pickle, datetime, sys

import par_functions as pf

#%% datos
print("\nExtracting avales ...   ", end="", flush=True)
# cuentas y tokens de moneda-par
accounts = pf.get_accounts()
tokens = pf.token_data()

#%% avales
# historial de propuesta-par
propuesta_history = pickle.load(open("output/raw/propuesta_history.p", "rb"))
# avales
avales = pf.get_avales_fromhistory(propuesta_history)
# enriquece/transforma df
avalesm = pf.transf_avalesdf(avales, accounts_df=accounts, tokens_df=tokens)

#%% save as pickle (because is raw)
avalesm.to_pickle("output/raw/avales_history_full.p")

print("[DONE]")
