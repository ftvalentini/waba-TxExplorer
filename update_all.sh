set +x
cd C:/Users/Fran/Documents/github/waba-TxExplorer

start=$(date +%s)

python 01-get_propuesta_historical.py &&
python 02-get_users_historical.py &&
python 03-get_avales.py &&
python 04-get_transactions.py &&
python 05-update_nodos_users.py &&
python 06-clean_data.py &&
python 07-timeseries_resumen.py &&
Rscript 08-update_html.R &&
gdrive-windows-x64.exe upload -p 1URJ-kx-NgSy0ecZMp9W6gkP4RE3CgMBi monedapar-resumen.html

end=$(date +%s)

dif=$((end-start))
hor=$((dif / 3600))
resto=$((dif % 3600))
min=$((resto / 60))
seg=$((resto % 60))
echo "Elapsed time: $hor hours $min minutes $seg seconds"

$SHELL
