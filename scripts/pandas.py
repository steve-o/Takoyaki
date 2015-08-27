#!/usr/bin/python

import urllib
import json
import pandas as pd

result = json.load(urllib.urlopen("http://nylabdev5:8000/FB.O/days?interval=2015-06-11T05:00:00.000Z/P5D"))
df = pd.DataFrame.from_items(zip(result['fields'], result['timeseries']))
df.datetime = df.datetime.astype("datetime64")
print df
