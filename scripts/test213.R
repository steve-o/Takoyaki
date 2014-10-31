library(RJSONIO)
library(RCurl)

json <- getURL("http://10.67.4.26:8000/GOOG.O?signal=SMA(14,Open())")
fromJSON(json)
