import os, sys
import getpass
from fernet import Fernet
import json
### Door Unlock
def tangerine():
    prefix = r"C:\Users"
    localuser = getpass.getuser()
    configsuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Sensitive\config.json"
    keysuffix = r"\Southeastern Computer Associates, LLC\GCA Deployment - Documents\Database\Daily Data Sets\Sensitive\key"
    configFile = prefix + "\\"+ localuser + configsuffix
    keyFile = prefix + "\\"+ localuser + keysuffix
    try:
        # print(os.getcwd())
        file = open(keyFile,'rb')
        key = file.read()
        file.close()
        with open(configFile,'rb') as f:
            config = f.read()
        f = Fernet(key)
        decrypted=f.decrypt(config)
        config = decrypted.decode("utf-8").replace("'", '"')
        config = json.loads(config)
        return config
    except Exception as e:
        print(e)
        return e

config = tangerine()
x=3