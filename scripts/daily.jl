#!/usr/bin/julia
using HTTPClient.HTTPC, JSON;
using DataFrames;
using Dates;

json = JSON.parse(bytestring(get("http://nylabdev5:8000/FB.O/days?interval=2015-06-11T05:00:00.000Z/P5D").body.data));
df = DataFrame(json["timeseries"], convert(Array{Symbol}, json["fields"]));
df[1] = convert(Array{DateTime}, map(x -> DateTime(x, "yyyy-mm-ddTHH:MM:SS.sZ"), df[1]));
println(df);
