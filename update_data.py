import os, sys, time

start = time.time()

os.system("python 01-get_propuesta_historical.py")
os.system("python 02-get_users_historical.py")
os.system("python 03-get_avales.py")
os.system("python 04-get_transactions.py")
os.system("python 05-update_nodos_users.py")
os.system("python 06-clean_data.py")
os.system("python 07-timeseries_resumen.py")

end = time.time()

h, resto = divmod(end-start, 3600)
m, s = divmod(resto, 60)
print(str(int(h))+' hours '+str(int(m))+' minutes '+str(int(s))+' seconds elapsed')
