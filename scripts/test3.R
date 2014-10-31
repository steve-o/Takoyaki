library(jsonlite)
library(httr)

json <- fromJSON("http://10.67.4.26:8000/NKE.N?signal=SMA(14,Open())")
