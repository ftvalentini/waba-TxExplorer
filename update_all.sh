set +x

start=$(date +%s)

python 01-get_propuesta_historical.py &&
python 02-get_users_historical.py &&
python 03-get_avales.py &&
python 04-get_transactions.py &&
python 05-update_nodos_users.py &&
python 06-update_saldos.py &&
python 07-clean_data.py &&
python 08-timeseries_resumen.py &&
Rscript 09-update_html.R &&
gdrive-windows-x64.exe update 1uoiWrrumrbiyi3Mk63hyNEMTuLCW7aHA output/final/html/monedapar-resumen.html

end=$(date +%s)
fecha=$(date +'%Y-%m-%d %H:%M:%S')

dif=$((end-start))
hor=$((dif / 3600))
resto=$((dif % 3600))
min=$((resto / 60))
seg=$((resto % 60))
printf "\nCompleted at $fecha"
printf "\nElapsed time: $hor hours $min minutes $seg seconds"

$SHELL

### NOTA: se puede necesitar correr esto en bash para que corra Rscript
# export R_LIBS_USER=C:/Users/Fran/Documents/R/win-library/3.5
# guia para gdrive: https://olivermarshall.net/how-to-upload-a-file-to-google-drive-from-the-command-line/
