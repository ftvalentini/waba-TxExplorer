import numpy as np
import pandas as pd
import pickle, sys, time

import par_functions as pf

# hoy = sys.argv[1]

#%% history of propuesta par (se va pisando a medida que se actualiza)
print('\nreading propuesta-par')
t0 = time.time()
propuesta_history = pf.get_propuesta_history()
t1 = time.time()
print('DONE in '+ str(round(t1-t0,1)) + ' seconds\n')
# guarda data en disco
pickle.dump(propuesta_history, open("output/raw/propuesta_history.p", "wb"))
