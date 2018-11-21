"%+%" <- function(a,b) paste(a,b,sep="")

cat("\nRendering html ...   ")
rmarkdown::render("nodoescobarmet_txs.Rmd", quiet=T,
                   output_file = "output/final/html/nodoescobarmet_"%+%Sys.Date()%+%".html")
cat("[DONE]\n\n")
