import numpy as np
import pandas as pd
import pickle, datetime

import get_functions as gf

# %% history of propuesta par
propuesta_history = gf.get_user_history(user_id='1.2.151476', max_page_num=99999999)
# guarda data en disco
hoy = datetime.datetime.today().date()
pickle.dump(propuesta_history, open("data/propuesta_history_"+str(hoy)+".p", "wb"))

# %% history of users
accounts = gf.get_accounts(prefix='moneda-par')
users_full_history = [gf.get_user_history(user_id=x, max_page_num=9999) for x in list(accounts.id_user)]
# convierte a series para que quede registrado id de cada historial
users_full_history = pd.Series(users_full_history, index=list(accounts.id_user))
# guarda data en disco
hoy = datetime.datetime.today().date()
pickle.dump(users_full_history, open("data/accounts_history_"+str(hoy)+".p", "wb"))
