# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 17:52:51 2022

@author: scedermas
"""
import pyodbc
import sqlite3
import pandas as pd


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
    
importa_gtr_tef()