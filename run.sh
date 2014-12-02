#!/bin/sh

# DACS login username, credentials define available services and usage limits.
DACS=
DACS=signals

# Infrastructure hostnames or IPv4 addresses.  Provide a list for support of
# round-robin failover.
PRIMARY_ADS=nylabads2
ADS_LIST=
ADS_LIST=$PRIMARY_ADS,nylabads1

# Signals App login UUID, concurrent logins typically kick oldest session.
UUID=PAXTRA77968

# Read from local files, leave empty to download from ADS.
# format: <field dictionary>,<enum dictionary>
DICTIONARY_OVERRIDE="RDMFieldDictionary"
DICTIONARY_OVERRIDE=

# Request timeout in seconds for absent NAK handling, default 60s.
RETRY_TIMER="10"
#RETRY_TIMER=
RETRY_LIMIT="3"
#RETRY_TIMER=

## -- end config --

# RSSL-only
SESSION="rssl://"
test -n "$DACS" && SESSION="$SESSION$DACS@"
SESSION="${SESSION}${PRIMARY_ADS}/?uuid=$UUID"
test -n "$ADS_LIST" && SESSION="$SESSION&server-list=$ADS_LIST"
test -n "$DICTIONARY_OVERRIDE" && SESSION="$SESSION&dictionary=$DICTIONARY_OVERRIDE"
test -n "$RETRY_TIMER" && SESSION="$SESSION&retry-timer=$RETRY_TIMER"
test -n "$RETRY_LIMIT" && SESSION="$SESSION&retry-limit=$RETRY_LIMIT"

./takoyaki.sh \
	"--session=$SESSION"

