Extension of project [Tako](https://github.com/steve-o/Tako) to support the Analytics domain.

Example:

```bash
curl "http://nylabdev5:8000/NKE.N?signal=MMA(21,Close())"
```

```javascript
{
    "type": "STATUS",
    "service": "ECP_SAP",
    "app": "TechAnalysis",
    "recordname": "NKE.N",
    "query": "MMA(21,Close())",
    "stream": "OPEN",
    "data": "NO_CHANGE",
    "code": "NONE",
    "text": " serviceName: TechAnalysis ricIntervalInfo:  ric: NKE.N
intervalInfo:  frequency: Daily intervalMultiplier: 1 formula: MMA(21,Close())
isStreamingRequest: 1 parameters:  name: UPA::IsPrivateStream value: 1
graphName:  ric: NKE.N intervalInfo:  frequency: Daily intervalMultiplier: 1
mma(21) map(close) mds keys:  ric:  intervalInfo:  frequency: None
intervalMultiplier: 1:  lookBack: 83 initPeriod: 100000 neededInputTypes:
close"
}
```
