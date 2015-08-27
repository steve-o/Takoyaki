#!/usr/bin/python
import urllib
import json
import numpy as np

result = json.load(urllib.urlopen("http://miru.hk/tmp/FB.O.json"))
r = np.core.records.fromarrays(result['timeseries'], names = result['fields'])
print r
