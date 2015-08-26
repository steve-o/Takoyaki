#!/usr/bin/python

import urllib
import json
import pandas as pd

result = json.load(urllib.urlopen("http://nylabdev5:8000/FB.O/days?interval=2015-06-11T05:00:00.000Z/P5D"))
df = pd.DataFrame(result["timeseries"], index=result["fields"])
print df
