# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 15:26:25 2022

@author: scedermas
"""

import pandas as pd
import sqlite3
import datetime as dt
import config
import requests

import pysftp

con = sqlite3.connect("wfm_nice.db")



customer=config.CUSTOMER
username=config.USERNAME
password=config.PASSWORD

 
def pedirToken():
    url="http://wfm19.atento.com/SMARTSync/services/Authentication"
    data={"customerName" : config.CUSTOMER, "userLogonID":config.USERNAME, "userLogonPWD":config.PASSWORD}
    headers = {"content-type" : "application/soap+xml"}
    r=requests.get(url=url, data=data, headers=headers)
    print("se solicitó nuevo token "+str(r))
    return r

def pedira():
    url="http://wfm19.atento.com/SMARTSync/services/Authentication"
    data={"customerName" : config.CUSTOMER, "userLogonID":config.USERNAME, "userLogonPWD":config.PASSWORD, "grant_type":"password"}
    # print(data)
    r=requests.post(url=url, data=data).json()
    print("se solicitó nuevo token ")
    return r

tk=pedirToken()