library(RJSONIO)
library(RCurl)

op <- options(digits.secs=3)
list <- fromJSON(getURL("http://nylabdev5:8000/FB.O/tas?interval=2015-06-11T05:00:00.000Z/P5D"))
list$timeseries[[2]] <- strptime(list$timeseries[[1]], "%Y-%m-%dT%H:%M:%OSZ", tz="GMT")
timeseries <- lapply(list$timeseries, function(x) {
	x[sapply(x, is.null)] <- NA
	unlist(x)
})
df <- data.frame(timeseries)
names(df) <- list$fields
print(df)
