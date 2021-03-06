library(RJSONIO)
library(RCurl)

op <- options(digits.secs=6)
list <- fromJSON(getURL("http://nylabdev5:8000/FB.O/tas?interval=2015-09-11T14:00:00.000Z/PT1S"), nullValue = NA)
list$timeseries[[1]] <- strptime(list$timeseries[[1]], "%Y-%m-%dT%H:%M:%OSZ", tz="GMT")
df <- data.frame(list$timeseries)
names(df) <- list$fields
print(df)
