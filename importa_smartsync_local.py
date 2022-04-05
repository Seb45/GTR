# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 15:26:25 2022

@author: scedermas
"""

import pandas as pd
import sqlite3
import datetime as dt


con = sqlite3.connect("wfm_nice.db")


ahora=dt.datetime.now()

dia="0"+str(ahora.day)
dia=dia[-2:]

mes="0"+str(ahora.month)
mes=mes[-2:]

df=pd.read_csv('agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt', delimiter="|", encoding='latin-1' )
df.to_sql("tmp_agent_schedule_detail", con, if_exists="replace")

df_rpt=pd.read_fwf('agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt.rpt', names=['mu', 'muid', 'guion', 'mu_descripcion'], skiprows=9, widths=[9,7,3,200], skipfooter=7, encoding='latin-1' )
df_rpt=df_rpt.drop(columns=['mu', 'guion'])
df_rpt.to_sql("tmp_mu_description", con, if_exists="replace")


cursorobj=con.cursor() 
cursorobj.execute('delete from agent_schedule_detail where schedDate in (select DISTINCT schedDate from tmp_agent_schedule_detail);')
con.commit() 


cursorobj.execute('insert into agent_schedule_detail select *, substr(tmp.externalID,1,2) from tmp_agent_schedule_detail tmp where schedDate is not null;')
con.commit() 

cursorobj.execute('delete from tmp_agent_schedule_detail;')
con.commit() 


cursorobj.execute('insert into mu_description select * from tmp_mu_description where muid not in (select muid from mu_description)')
con.commit() 

cursorobj.execute('delete from tmp_mu_description;')
con.commit() 

cursorobj.execute('delete from agent_schedule_detail_hoy;')
con.commit() 

cursorobj.execute('insert into agent_schedule_detail_hoy select * from agent_schedule_detail where TZ like "%Buenos_Aires%" AND julianday(substr(schedDate,4, 4)||"-0"||substr(schedDate,1, 1)||"-"||substr(schedDate,2, 2))=julianday(date("now", "localtime") )')
con.commit() 


