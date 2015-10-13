#!/usr/bin/julia
# If a proxy is required, set https_proxy and then the following:
# git config --global url."https://github.com/".insteadOf git://github.com/
Pkg.add("Requests")
Pkg.add("JSON");
Pkg.add("DataFrames");
Pkg.add("Dates");
