# -*- coding: utf-8 -*-
"""
Created on Fri Feb 11 15:26:25 2022

@author: scedermas
"""

import pandas as pd
import sqlite3
import datetime as dt
import pyodbc

import pysftp

con = sqlite3.connect("wfm_nice.db")


def importa_nomina_sql():
    conn = pyodbc.connect('DSN=mtzw19cltp07_cdi_64; UID=powerbi; PWD=powerbi')
    
    cursor = conn.cursor()
    
    df_nomina=pd.read_sql('SELECT * FROM nomina WHERE fecha_carga=cast(floor(cast(getdate() as decimal(9,4))) as smalldatetime)', conn)
    df_nomina.to_sql("nomina", con, if_exists="replace")
    print("actualizado")
    

def importa_nomina_xls():
    data_xls=pd.read_excel ('informe_18.xlsx')
    data_xls.to_sql("reporte_18", con, if_exists="replace")
    

def importa_gtr_tef():
    data_gtr=pd.read_csv ('I:/Planeamiento/07.MIS/10.Bajadas/agentes gtr/agente_movi_gtr.txt', skiprows=1, sep='\t'   )
    data_gtr.to_sql("gtr_tef", con, if_exists="replace")
    
importa_nomina_sql()


hostname="10.158.157.11"
# sftp_username="sftp_peru"
# sftp_pw="6PKbOepF"

sftp_username="sftp_plan_arg"
sftp_pw="@rG3nT1n@232"

 

# sftp_username="sftp_plan_chi"
# sftp_pw="Ch1L3@#12321"


ahora=dt.datetime.now()

dia="0"+str(ahora.day)
dia=dia[-2:]

mes="0"+str(ahora.month)
mes=mes[-2:]


cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(hostname,
                       username=sftp_username,
                       password=sftp_pw,
                       cnopts = cnopts
                       ) as sftp:

    file = sftp.get('/exports/customer1/agentScheduleDetail_Day/agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt')
    file = sftp.get('/exports/customer1/agentScheduleDetail_Day/agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt.rpt')



sftp.close()


df=pd.read_csv('agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt', delimiter="|")
df.to_sql("tmp_agent_schedule_detail", con, if_exists="replace")

df_rpt=pd.read_fwf('agentScheduleDetail_'+str(dia)+str(mes)+str(ahora.year)+'.txt.rpt', names=['mu', 'muid', 'guion', 'mu_descripcion'], skiprows=9, widths=[9,7,3,200], skipfooter=7)
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


