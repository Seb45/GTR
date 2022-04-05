
import datetime as dt
import time

from subprocess import call

import os 
  
# Command to execute
# Using Windows OS command
cmd = 'python "C:/Users/scedermas/Documents/wfm/importa_gtr.py"'

while True:
    while (dt.datetime.now().minute%5!=3):
        time.sleep(7)
 
    try:
        os.system(cmd)
        time.sleep(59)

    except:
        pass
