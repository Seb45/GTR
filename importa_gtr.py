
import sqlite3
import pandas as pd
import datetime as dt
import os.path, time
import warnings
warnings.filterwarnings("ignore")

con = sqlite3.connect("wfm_nice.db")

def importa_txt(archivo):

    cursorobj=con.cursor()
    # cursorobj.execute('delete from gtr_'+archivo';')
    # con.commit()
    data_gtr=pd.read_csv ('I:/Planeamiento/07.MIS/10.Bajadas/agentes gtr/'+archivo+'.txt', skiprows=1, sep='\t', encoding='latin-1'   )
    data_gtr.to_sql("tmp_gtr_"+archivo, con, if_exists="replace")
    # cursorobj.execute('delete from gtr_'+archivo+';')
    # con.commit()
    # cursorobj.execute('insert into gtr_'+archivo+' select * from tmp_gtr_'+archivo+';')
    # con.commit()
    

importa_txt("agente_tlc_gtr")
importa_txt("agente_tlc_gtr1")
importa_txt("agente_tlc_gtr2")
importa_txt("agente_movi_gtr")

importa_txt("agente_visa_gtr")
importa_txt("agente_visa_gtr1")
importa_txt("agente_visa_gtr2")
importa_txt("agente_visa_gtr3")

importa_txt("agente_multi_gtr")


cursorobj=con.cursor()
cursorobj.execute('delete from gtr_tef;')
con.commit()
# data_gtr=pd.read_csv ('I:/Planeamiento/07.MIS/10.Bajadas/agentes gtr/agente_movi_gtr.txt', skiprows=1, sep='\t', encoding='latin-1'   )
# data_gtr.to_sql("tmp_gtr_tef", con, if_exists="replace")
# cursorobj.execute('delete from gtr_tef;')
# con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_tlc_gtr;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_tlc_gtr1;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_tlc_gtr2;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_movi_gtr;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_visa_gtr;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_visa_gtr1;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_visa_gtr2;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_visa_gtr3;')
con.commit()
cursorobj.execute('insert into gtr_tef select * from tmp_gtr_agente_multi_gtr;')
con.commit()

print(dt.datetime.now())

fecha_act=time.ctime(os.path.getmtime("I:/Planeamiento/07.MIS/10.Bajadas/agentes gtr/agente_movi_gtr.txt"))

print(fecha_act)
cursorobj=con.cursor()
cursorobj.execute('update act_txt_tef set fecha="'+str(fecha_act)+'";')
con.commit()

cursorobj.execute('insert into info_progra_ocup_util_histo  select datetime("now", "localtime") as fechahora, * from s_status_gtr_programa_estado_AVAIL')
con.commit()

