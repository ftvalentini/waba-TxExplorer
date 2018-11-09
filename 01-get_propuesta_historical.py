import numpy as np
import pandas as pd
import pickle, datetime, sys

import par_functions as pf

# hoy = sys.argv[1]

#%% history of propuesta par (se va pisando a medida que se actualiza)
propuesta_history = pf.get_propuesta_history()
# guarda data en disco
pickle.dump(propuesta_history, open("output/raw/propuesta_history.p", "wb"))
