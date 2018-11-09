import os, sys, time

start = time.time()

os.system("py -3 01-get_propuesta_historical.py")
os.system("py -3 02-get_users_historical.py")
os.system("py -3 03-get_avales.py")
os.system("py -3 04-get_transactions.py")
os.system("py -3 05-update_nodos_users.py")
os.system("py -3 06-clean_data.py")
os.system("py -3 07-timeseries_resumen.py")

end = time.time()

h, resto = divmod(end-start, 3600)
m, s = divmod(resto, 60)
print(str(int(h))+' hours '+str(int(m))+' minutes '+str(int(s))+' seconds elapsed')
