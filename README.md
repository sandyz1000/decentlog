# Decentralized logging using dweet, Logging from one server in dweet.io channel and listen on your local client  

## Installation


```
$ pip install git+https://github.com/sandyz1000/decentlog.git
```

Run ttyd server on Google Colab or Kaggle Notebooks

## Getting Started


Decent also has a command-line script. So you can just run `decent` from command line.

`decent -h` will give the following:

```
usage: decent [-h] --mode PUBLISHER --channel channel-name

ColabShell: Run TTYD server On Colab / Kaggle Notebooks to access the GPU machine from SHELL

required arguments:
  --mode PUBLISHER 
  --channel CHANNEL-NAME dweet channel name that you want to subscribe for logging

optional arguments:
  --credential CREDENTIAL  username and password to protect your shell from unauthorized access, format username:password
```

## Logging from Kaggle Notebook and listen to your local client

```
from colabshell import ColabShell
shell = ColabShell(port=10001, username='sandip', password='pass123@', mount_drive=True)
shell.run()
```

References: 
https://dweet.io/
https://github.com/paddycarey/dweepy