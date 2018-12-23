set +x

start=$(date +%s)

python saldos_escobarmet.py &&
Rscript escobarmet_html.R

end=$(date +%s)
fecha=$(date +'%Y-%m-%d %H:%M:%S')

dif=$((end-start))
min=$((dif / 60))
seg=$((dif % 60))

printf "\nCompleted at $fecha"
printf "\nElapsed time: $min minutes $seg seconds"

$SHELL

### NOTA: se puede necesitar correr esto en bash para que corra Rscript
# export R_LIBS_USER=C:/Users/Fran/Documents/R/win-library/3.5
### guia para gdrive: https://olivermarshall.net/how-to-upload-a-file-to-google-drive-from-the-command-line/
