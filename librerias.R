
# escribir paquetes "desiempre" y "proj" ---------------------------------------
# las desiempre no se llaman explicitamente en los Scripts, las proj sí
  paq_desiempre <- c(
    "magrittr",
    "purrr",
    "conflicted",
    "ggplot2",
    "dplyr",
    "stringr",
    "broom",
    "knitr",
    "readr",
    "kableExtra",
    "rmarkdown",
    "DT"
  )
paq_proj <- c(
  )

# no tocar ----------------------------------------------------------------
paq <- c(paq_desiempre,paq_proj)
for (i in seq_along(paq)) {
  if (!(paq[i] %in% installed.packages()[,1])) {
    install.packages(paq[i], quiet=T, verbose=F,
      repos="http://cran.us.r-project.org")
  }
  if (paq[i] %in% paq_desiempre) library(paq[i],character.only=T,
                                         quietly=T,warn.conflicts=F,verbose=F)
}
rm(paq,paq_desiempre,paq_proj)
