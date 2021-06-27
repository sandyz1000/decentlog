# Decentralized logging using dweet.io API.

Logging from remote/notebook server in dweet.io channel and listen on your local client  

## Installation

```
$ pip install git+https://github.com/sandyz1000/decentlog.git
```

## Getting Started

Decent also has a command-line script so you can simply run `decent_cli` from command line.

`decent_cli -h` will give the following:

```
usage: decent_cli [-h] --mode LISTENER --channel channel-name --polling False

DecentLog: Decentralized logging mechanism using dweet.io, useful if you want to log from remote notebook 
server to local workstation 

required arguments:
  --channel CHANNEL-NAME dweet channel name that you want to subscribe for logging

optional arguments:
  --mode LISTENER (default)
  --polling (default=False) Use polling instead of streaming
```

## Logging from remote server and listen on your local client

```
from decentlog import init_decentlog

init_decentlog("this_is_a_channel")
print("Testing logging from decentlog !!!")
```

## On your local workstation you can listen to the log message using

```
decent_cli --channel "this_is_a_channel"
```

###### References:

https://dweet.io/
https://github.com/paddycarey/dweepy
