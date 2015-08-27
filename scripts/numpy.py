#!/usr/bin/python
import urllib
import json
import numpy as np

result = json.load(urllib.urlopen("http://nylabdev5:8000/FB.O/days?interval=2015-06-11T05:00:00.000Z/P5D"))
r = np.core.records.fromarrays(result['timeseries'], names = result['fields'])
print r
