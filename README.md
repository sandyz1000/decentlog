# Decentralized logging using dweet.
# Logging from remote/notebook server in dweet.io channel and listen on your local client  

## Installation


```
$ pip install git+https://github.com/sandyz1000/decentlog.git
```

Run Decent log client to look for output

## Getting Started


Decent also has a command-line script. So you can just run `decent` from command line.

`decent -h` will give the following:

```
usage: decent [-h] --mode PUBLISHER --channel channel-name

DecentLog: Decentralized logging mechanism using dweetio, useful if you want to log from remote notebook 
server to local workstation 

required arguments:
  --mode PUBLISHER (default)
  --channel CHANNEL-NAME dweet channel name that you want to subscribe for logging

```

## Logging from remote server and listen to your local client

```
from decentlog import init_decentlog

init_decentlog("this_is_a_channel")
print("Testing logging from decentlog !!!")

```

## And on your local m/c you can listen to the log message using

```
decent_cli --channel "this_is_a_channel"
```

References: 
https://dweet.io/
https://github.com/paddycarey/dweepy
