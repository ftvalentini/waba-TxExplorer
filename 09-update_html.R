cat("\nRendering html ...   ")
rmarkdown::render("monedapar-resumen.Rmd", quiet=T,
                   output_file = "output/final/html/monedapar-resumen.html")
cat("[DONE]\n\n")
