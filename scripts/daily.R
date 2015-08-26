library(RJSONIO)
library(RCurl)

op <- options(digits.secs=3)
list <- fromJSON(getURL("http://nylabdev5:8000/FB.O/days?interval=2015-06-11T05:00:00.000Z/P5D"), nullValue = NA)
list$timeseries[[1]] <- strptime(list$timeseries[[1]], "%Y-%m-%dT%H:%M:%OSZ", tz="GMT")
df <- data.frame(list$timeseries)
names(df) <- list$fields
print(df)
