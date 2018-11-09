#%%
import numpy as np
import pandas as pd
import pickle, sys

import par_functions as pf

#%% history of users
accounts = pf.get_accounts()
users_full_history = []
for i in range(len(accounts.id)):
    print('reading user '+str(i)+' of '+str(len(accounts.id)))
    history = pf.get_user_history(user_id=accounts.id[i])
    print('user '+str(i)+' DONE')
    users_full_history += history
# convierte a series para que quede registrado id de cada historial
users_full_history = pd.Series(users_full_history, index=list(accounts.id))
# guarda data en disco
pickle.dump(users_full_history, open("output/raw/accounts_history.p", "wb"))
