# Python Notebook - Resumen movimientos de Papel FSC

import pandas as pd

from itertools import chain

from sqlalchemy import create_engine, text

from datetime import datetime

import os

dateFilter_byDate_F = False

def set_datefilter(Filter = False):

  global dateFilter_byDate_F
  dateFilter_byDate_F = Filter



def db_connectionObj():
  db_connection_str = 'mysql+pymysql://reports:cognos@192.168.1.238/pruebas?charset=utf8'

  db_connection = create_engine(db_connection_str)

  return db_connection



def trazabilidad(self, ops, db_connection):

  query_str = """SELECT j.j_number AS OP ,CONCAT(j.j_title1, IFNULL(j.j_title2,'')) AS titulo, CASE
  WHEN tr.wt_resource LIKE \'GUILL%\' then \'Convertidora\'
  WHEN tr.wt_resource LIKE \'%PEG CAJ%\' then \'Pegado de Cajas\'
  WHEN tr.wt_resource LIKE \'PRE %\' then \'Prensas\'
  WHEN tr.wt_resource LIKE \'%TRO%\' then \'Troquel\'
  ELSE \'Revisado\' END AS \'Proceso\' , tr.wt_source_code AS Operario, tr.wt_started AS Fecha
  FROM job200 j
  INNER JOIN wo200 w ON j.j_number = w.wo_job
  LEFT JOIN wo_task200 tk ON w.wo_number = tk.tk_wonum 
  LEFT JOIN wo_trans200 tr ON tk.tk_id = tr.wt_task_id
  WHERE tr.wt_source = \'TS\' AND
  (tk.tk_code LIKE \'%TIR%\' OR tk.tk_code LIKE \'%REVISADO%\')
  AND j.j_number IN	({_list})
  GROUP BY j.j_number, tr.wt_resource, tr.wt_source_code
  ORDER BY j.j_number;""".format(_list=','.join(['\'{}\''.format(op) for op in ops]))

  query_str1 = """SELECT ist.ist_job AS OP,inv.inv_date as Fecha_Factura, CONCAT(inv.inv_prefix,inv.inv_number) as no_factura
		FROM inv
		INNER JOIN ist ON 
		inv.inv_id = ist.ist_inv_id
		WHERE ist.ist_job IN ({_list})""".format(_list=','.join(['\'{}\''.format(op) for op in ops]))

  try:
    df_actividades = pd.read_sql_query(text(query_str), con = db_connection)

    df_facturas = pd.read_sql_query(text(query_str1), con = db_connection)

    if len(df_actividades.index) == 0:
      return None

    df_procesos = df_actividades.loc[:,['OP','Proceso','Operario','Fecha']]

    df_facturas = pd.merge(left=df_facturas, right=df_actividades.loc[:,['OP','titulo']], how='right', on='OP')

    df_facturas.drop_duplicates(subset=['no_factura'], inplace=True)

  
    Ops = df_procesos.OP.unique()

    def format_title(s):
      if "OP" in s:
        return s
      else:
        return s+"_{}".format(proceso)
    df_trazabilidad = pd.DataFrame()
    df_list = list()
    for OP in Ops:
      dfOp = df_procesos[df_procesos.OP == OP]
      procesos = df_procesos["Proceso"].unique()
      proceso_df = pd.DataFrame()
      for index, proceso in enumerate(procesos):
        temp_df = dfOp[dfOp.Proceso == proceso]
        temp_df = temp_df.drop_duplicates(subset=["Operario"])
        if not proceso_df.empty:
          temp_df = temp_df.rename(format_title, axis='columns').drop(columns = ['OP'])
          proceso_df = pd.concat([proceso_df.reset_index(drop=True), temp_df.reset_index(drop=True)],axis=1)
        else:
          proceso_df = temp_df.rename(format_title, axis='columns')

      df_list.append(proceso_df.reset_index(drop=True))

      
    
    df_trazabilidad = pd.concat(df_list, axis=0)

    listProcesos = ['Convertidora','Prensas','Troquel','Pegado de Cajas','Revisado']

    orderList = list(chain.from_iterable((f'Proceso_{Proceso}',f'Operario_{Proceso}',f'Fecha_{Proceso}') for Proceso in listProcesos if Proceso in procesos))

    df_trazabilidad = pd.merge(df_trazabilidad, right=df_facturas, how='left', on='OP') 
    
    df_trazabilidad = df_trazabilidad.loc[:,['OP','titulo','Fecha_Factura','no_factura'] + orderList]

      
          
    return df_trazabilidad.fillna(0)
  except Exception as e:
    print("Se dió un problema en trazabilidad: {}".format(e))
    return

def movimientos(self,args,path, db_connection):

  query_str1 = """SELECT
  Tabla1.*, Facturado.Qty, Facturado.CQty
  FROM
  (
    SELECT
      jb.j_number AS OP,
      CONCAT(jb.j_title1, IFNULL(jb.j_title2,'')) as titulo,
      jb.j_type AS 'Tipo Producto',
      tk_code,
      jb.j_ucode1 AS 'Categoría FSC',
      SUM(tr.wt_good_qty) AS Cantidad_Buenas,
      SUM(tr.wt_bad_qty) AS Cantidad_Malas,
      jb.j_special_ins AS Datos_Papel,
      cons_bodega.quantity AS Despacho_Bodega,
      cons_bodega.Ancho,
      cons_bodega.Alto,
      cons_bodega.Gramaje,
      cons_bodega.Nota,
      jb.j_ucode4 AS Peso_Ejemplar
    FROM
      job200 jb
      INNER JOIN wo200 wo ON jb.j_number = wo.wo_job
      INNER JOIN wo_task200 tsk ON wo.wo_number = tsk.tk_wonum
      LEFT JOIN wo_trans200 tr ON tsk.tk_id = tr.wt_task_id
      LEFT JOIN (
        SELECT
          job200.j_number,
          SUM(iss.quantity) AS quantity,
          stk.itm_fvals_0 AS "Ancho",
          stk.itm_fvals_1 AS "Alto",
          stk.itm_fvals_2 AS "Gramaje",
          GROUP_CONCAT(IFNULL(iss.note,'') SEPARATOR ' ') AS "Nota"
        FROM
          req
          INNER JOIN wo_task200 tk ON req.req_task_id = tk.tk_id
          INNER JOIN wo200 ON tk.tk_wonum = wo200.wo_number
          INNER JOIN job200 ON job200.j_number = wo200.wo_job
          INNER JOIN iss ON req.id = iss.req_id
          INNER JOIN itm_cls_view itm_ ON iss.item = itm_.itm_code
          INNER JOIN stkitm stk ON itm_.itm_code = stk.itm_code
        WHERE
          itm_.itm_is_paper = 1
          AND YEAR(job200.j_booked_in) >= 2021
        GROUP BY
          job200.j_number
      ) AS cons_bodega ON jb.j_number = cons_bodega.j_number
    WHERE
      tsk.tk_code IN (
        'PRE XL-TIR',
        'PRE CX-TIR',
        'PRE SM-TIR',
        'PCAJ FON G',
        'PCAJ FON M',
        'PCAJ FON P',
        'PCAJ LAT G',
        'PCAJ LAT P',
        'PCAR N',
        'PCAR TS',
        'PEGTT',
        'PINS SISA',
        'H TIR 4ESQ',
        'H TIR FG',
        'H TIR FM',
        'H TIR FP',
        'H TIR LG',
        'H TIR LP',
        'D TIR 2L',
        'D TIR 4ESQ',
        'D TIR FG',
        'D TIR FM',
        'D TIR FP',
        'D TIR LG',
        'D TIR LP',
        'TROPEQ TIR',
        'TROMED TIR',
        'TROGRD TIR',
        'TROPLA TIR',
         'REVISADO',
        'REVISADO2',
        'REVISADO3',
        'REVISADO 4',
        'REVISADO5',
        'GUILL-TIR'
        
      )
      AND wt_source = 'TS'
      AND jb.j_ucode1 IS NOT NULL
    GROUP BY
      jb.j_number,
      tsk.tk_code
  ) AS Tabla1 LEFT JOIN (
    SELECT j.j_number, chrge.CQty, SUM(ist.ist_quantity) AS Qty, inv.inv_date AS inv_date
		FROM job200 j
		LEFT JOIN ist ON 
		j.j_number = ist.ist_job
		LEFT JOIN inv ON ist.ist_inv_id = inv.inv_id
		INNER JOIN ( SELECT job200.j_number, SUM(c.cg_quantity) AS CQty
		FROM job200 
		INNER JOIN charge c ON job200.j_number = c.cg_job
    WHERE YEAR(c.cg_date_created) >= 2021
		GROUP BY job200.j_number) AS chrge ON j.j_number = chrge.j_number
    WHERE YEAR(j.j_booked_in) >= 2021
		GROUP BY j.j_number
  ) AS Facturado ON Tabla1.OP = Facturado.j_number
   WHERE Facturado.inv_date BETWEEN {startdate} AND {endate};""".format(startdate=args[0], endate= args[1] if len(args) > 1 else args[0])

  query_str2 = """SELECT
  Tabla1.*, Facturado.Qty, Facturado.CQty
  FROM
  (
    SELECT
      jb.j_number AS OP,
      CONCAT(jb.j_title1, IFNULL(jb.j_title2,'')) as titulo,
      jb.j_type AS 'Tipo Producto',
		jb.j_ucode1 AS 'Categoría FSC',
      tk_code,
      SUM(tr.wt_good_qty) AS Cantidad_Buenas,
      SUM(tr.wt_bad_qty) AS Cantidad_Malas,
      jb.j_special_ins AS Datos_Papel,
      cons_bodega.quantity AS Despacho_Bodega,
      cons_bodega.Ancho,
      cons_bodega.Alto,
      cons_bodega.Gramaje,
      cons_bodega.Nota,
      jb.j_ucode4 AS Peso_Ejemplar
    FROM
      job200 jb
      INNER JOIN wo200 wo ON jb.j_number = wo.wo_job
      LEFT JOIN wo_task200 tsk ON wo.wo_number = tsk.tk_wonum
      LEFT JOIN wo_trans200 tr ON tsk.tk_id = tr.wt_task_id
      LEFT JOIN (
        SELECT
          job200.j_number,
          SUM(iss.quantity) AS quantity,
          stk.itm_fvals_0 AS "Ancho",
          stk.itm_fvals_1 AS "Alto",
          stk.itm_fvals_2 AS "Gramaje",
          GROUP_CONCAT(IFNULL(iss.note,'') SEPARATOR ' ') AS "Nota"
        FROM
          req
          INNER JOIN wo_task200 tk ON req.req_task_id = tk.tk_id
          INNER JOIN wo200 ON tk.tk_wonum = wo200.wo_number
          INNER JOIN job200 ON job200.j_number = wo200.wo_job
          INNER JOIN iss ON req.id = iss.req_id
          INNER JOIN itm_cls_view itm_ ON iss.item = itm_.itm_code
          INNER JOIN stkitm stk ON itm_.itm_code = stk.itm_code
        WHERE
          itm_.itm_is_paper = 1
          AND YEAR(job200.j_booked_in) >= 2021
        GROUP BY
          job200.j_number
      ) AS cons_bodega ON jb.j_number = cons_bodega.j_number
    WHERE
      jb.j_ucode1 IS NOT NULL
      AND jb.j_booked_in BETWEEN {startdate} AND {endate}
    GROUP BY
      jb.j_number,
      tsk.tk_code
  ) AS Tabla1 LEFT JOIN (
    SELECT j.j_number, chrge.CQty, SUM(ist.ist_quantity) AS Qty, inv.inv_date AS inv_date
		FROM job200 j
		LEFT JOIN ist ON 
		j.j_number = ist.ist_job
		LEFT JOIN inv ON ist.ist_inv_id = inv.inv_id
		LEFT JOIN ( SELECT job200.j_number, SUM(c.cg_quantity) AS CQty
		FROM job200 
		LEFT JOIN charge c ON job200.j_number = c.cg_job
    WHERE YEAR(c.cg_date_created) >= 2021
		GROUP BY job200.j_number) AS chrge ON j.j_number = chrge.j_number
    WHERE YEAR(j.j_booked_in) >= 2021
		GROUP BY j.j_number
  ) AS Facturado ON Tabla1.OP = Facturado.j_number;""".format(startdate=args[0], endate= args[1] if len(args) > 1 else args[0])


  query_str3 = """SELECT
  Tabla1.*, Facturado.Qty, Facturado.CQty
  FROM
  (
    SELECT
      jb.j_number AS OP,
      CONCAT(jb.j_title1, IFNULL(jb.j_title2,'')) as titulo,
      jb.j_type AS 'Tipo Producto',
      tk_code,
      jb.j_ucode1 AS 'Categoría FSC',
      SUM(tr.wt_good_qty) AS Cantidad_Buenas,
      SUM(tr.wt_bad_qty) AS Cantidad_Malas,
      jb.j_special_ins AS Datos_Papel,
      cons_bodega.quantity AS Despacho_Bodega,
      cons_bodega.Ancho,
      cons_bodega.Alto,
      cons_bodega.Gramaje,
      cons_bodega.Nota,
      jb.j_ucode4 AS Peso_Ejemplar
    FROM
      job200 jb
      INNER JOIN wo200 wo ON jb.j_number = wo.wo_job
      INNER JOIN wo_task200 tsk ON wo.wo_number = tsk.tk_wonum
      LEFT JOIN wo_trans200 tr ON tsk.tk_id = tr.wt_task_id
      LEFT JOIN (
        SELECT
          job200.j_number,
          SUM(iss.quantity) AS quantity,
          stk.itm_fvals_0 AS "Ancho",
          stk.itm_fvals_1 AS "Alto",
          stk.itm_fvals_2 AS "Gramaje",
          GROUP_CONCAT(IFNULL(iss.note,'') SEPARATOR ' ') AS "Nota"
        FROM
          req
          INNER JOIN wo_task200 tk ON req.req_task_id = tk.tk_id
          INNER JOIN wo200 ON tk.tk_wonum = wo200.wo_number
          INNER JOIN job200 ON job200.j_number = wo200.wo_job
          INNER JOIN iss ON req.id = iss.req_id
          INNER JOIN itm_cls_view itm_ ON iss.item = itm_.itm_code
          INNER JOIN stkitm stk ON itm_.itm_code = stk.itm_code
        WHERE
          itm_.itm_is_paper = 1
          AND YEAR(job200.j_booked_in) >= 2021
        GROUP BY
          job200.j_number
      ) AS cons_bodega ON jb.j_number = cons_bodega.j_number
    WHERE
      jb.j_ucode1 IS NOT NULL 
      AND jb.j_number IN ({_list})
    GROUP BY
      jb.j_number,
      tsk.tk_code
  ) AS Tabla1 LEFT JOIN (
    SELECT j.j_number, chrge.CQty, SUM(ist.ist_quantity) AS Qty, inv.inv_date AS inv_date
		FROM job200 j
		LEFT JOIN ist ON 
		j.j_number = ist.ist_job
		LEFT JOIN inv ON ist.ist_inv_id = inv.inv_id
		INNER JOIN ( SELECT job200.j_number, SUM(c.cg_quantity) AS CQty
		FROM job200 
		INNER JOIN charge c ON job200.j_number = c.cg_job
    WHERE YEAR(c.cg_date_created) >= 2021
		GROUP BY job200.j_number) AS chrge ON j.j_number = chrge.j_number
    WHERE YEAR(j.j_booked_in) >= 2021
		GROUP BY j.j_number
  ) AS Facturado ON Tabla1.OP = Facturado.j_number;""".format(_list=args)

  global dateFilter_byDate_F

  if type(args) is list:
    if dateFilter_byDate_F:
      query_str = query_str1
    else:
      query_str = query_str2
  else:
    query_str = query_str3

  try:
    df = pd.read_sql_query(text(query_str), con = db_connection)
    
    self.progress.emit(25)

    if(len(df.index) == 0):
      self.message.emit("No se encontraron datos!")
      return

    df.Datos_Papel.fillna("", inplace=True)

    df.fillna(0.0,inplace=True)

    df["Peso_Ejemplar"] = pd.to_numeric(df["Peso_Ejemplar"])/1000

    def set_charge (row):
      if(row['Qty'] == 0.0):
        return row['CQty']
      else:
        return row['Qty']

    df['Qty'] = df.apply(set_charge,axis=1)

    df.drop(columns='CQty', inplace=True)

    df_medidas = df.copy()
    df_medidas.drop(['Nota','titulo','Tipo Producto','Categoría FSC','Cantidad_Buenas', 'Cantidad_Malas','tk_code','Despacho_Bodega', 'Peso_Ejemplar', 'Qty'], axis=1, inplace=True)

    df_medidas["Dimensiones Prensa"] = df_medidas.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[0]

    df_medidas["Número_Pliegos"] = pd.to_numeric(df_medidas.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[1].str.split(" ", expand=True)[2])

    df_medidas.drop(columns=["Datos_Papel"], inplace=True)

    pd.set_option('display.max_colwidth', None)

    self.progress.emit(30)

    df_datos_prensas = df.copy()

    df_datos_prensas.drop(['Nota','titulo','Tipo Producto','Categoría FSC','Datos_Papel','Ancho', 'Alto', 'Gramaje','Peso_Ejemplar', 'Qty'], axis=1, inplace=True)

    df_datos_prensas = df_datos_prensas[df_datos_prensas["tk_code"].isin(["PRE CX-TIR","PRE XL-TIR", "PRE SM-TIR", "PREIND TIR" ])]

    def calc_area(x):
      measures  = [int(s)/1000 for s in str(x).split() if s.isdigit()]
      return pd.Series(measures, dtype='float64')
      
    df_medidas[["Alto Prensa", "Ancho Prensa"]] = df_medidas["Dimensiones Prensa"].apply(calc_area)

    df_medidas.drop(columns=["Dimensiones Prensa"], inplace=True)

    df_medidas.Ancho = df_medidas.Ancho.transform(lambda x: x/1000)

    df_medidas.Alto = df_medidas.Alto.transform(lambda x: x/1000)

    df_medidas.Gramaje = df_medidas.Gramaje.transform(lambda x: x/1000)

    df_medidas['Área Pliego Almacén'] = df_medidas.Ancho * df_medidas.Alto

    df_medidas['Área Pliego Prensa'] = df_medidas['Alto Prensa'] * df_medidas['Ancho Prensa']

    df_medidas.drop(columns=['Alto','Ancho', 'Alto Prensa', 'Ancho Prensa'], inplace=True)

    df_medidas['Masa por Pliego Almacén'] = df_medidas['Área Pliego Almacén']*df_medidas.Gramaje

    df_medidas['Masa por Pliego Prensa'] = df_medidas['Área Pliego Prensa'] * df_medidas.Gramaje
    

    df_medidas = df_medidas.drop_duplicates(subset=['OP'])


    df_datos_prensas.insert(4, 'Cantidad_Total', df_datos_prensas['Cantidad_Buenas']+df_datos_prensas['Cantidad_Malas'])

    self.progress.emit(40)


    df_datos_comb = pd.merge(left=df_datos_prensas, right=df_medidas, how='left', left_on='OP', right_on='OP')

    df_guillotina = df.copy()

    df_guillotina.drop(['Nota','titulo','Categoría FSC','Tipo Producto','Datos_Papel','Ancho', 'Alto', 'Gramaje','Peso_Ejemplar', 'Qty'], axis=1, inplace=True)

    df_guillotina = df_guillotina[df_guillotina["tk_code"].isin(["GUILL-TIR"])]

    df_guillotina =  pd.merge(left=df_guillotina, right=df_medidas, how='left', left_on='OP', right_on='OP')

    df_guillotina.drop_duplicates(subset=['OP'])

    df_guillotina['Masa Perdida (kg)'] = df_guillotina['Cantidad_Malas'] * df_guillotina['Masa por Pliego Almacén']

    df_guillotina = df_guillotina[['OP','Masa Perdida (kg)']]

    df_datos = pd.merge(left=df_datos_comb, right=df_guillotina,how='left', on='OP')

    df_despacho = df_datos.copy()

    df_despacho = df_despacho.drop_duplicates(subset=['OP'])

    df_despacho.drop(columns=['Masa por Pliego Prensa', 'Cantidad_Buenas', 'Cantidad_Malas', 'Cantidad_Total'], inplace=True)

    df_despacho['Pérdida Corte Inicial por exceso (Kg)'] = (((df_despacho['Área Pliego Almacén'] - (df_despacho['Área Pliego Prensa']*df_despacho['Número_Pliegos'])) * df_despacho['Gramaje']) * df_despacho.Despacho_Bodega)  + df_despacho['Masa Perdida (kg)']

    df_despacho['Despachos de pliego almacén (Kg)'] = df_despacho.Despacho_Bodega * df_despacho['Masa por Pliego Almacén']

    df_despacho = df_despacho[['OP','Despacho_Bodega','Despachos de pliego almacén (Kg)','Pérdida Corte Inicial por exceso (Kg)']]

    df_despacho['Fraccupib de pérdida por Corte Inicial (%)'] = df_despacho['Pérdida Corte Inicial por exceso (Kg)'] / df_despacho['Despachos de pliego almacén (Kg)']

    df_datos_pre = df_datos_comb.copy()

    df_datos_pre = df_datos_pre.drop_duplicates(subset=['OP'])

    df_datos_pre.drop(columns=['Gramaje','tk_code','Área Pliego Almacén','Área Pliego Prensa', 'Despacho_Bodega'], inplace=True)


    self.progress.emit(50)

    df_impresion = pd.merge(left=df_despacho, right=df_datos_pre, how="left", left_on="OP", right_on="OP")

    df_impresion.insert(8,"Material para Arreglos e Impresión (Kg)", df_impresion['Despachos de pliego almacén (Kg)'] - df_impresion['Pérdida Corte Inicial por exceso (Kg)'])

    df_impresion['Pérdida por arreglo de Impresión (Kg)'] = df_impresion['Cantidad_Malas'] * df_impresion['Masa por Pliego Prensa']

    df_impresion['Pliegos para Arreglo e Impresión'] = round(df_impresion['Material para Arreglos e Impresión (Kg)'] / df_impresion['Masa por Pliego Prensa'],0)

    df_impresion['Fracción de pérdida por Impresión (%)'] = df_impresion['Pérdida por arreglo de Impresión (Kg)'] / df_impresion['Material para Arreglos e Impresión (Kg)']

    df_impresion.drop(columns=['Cantidad_Total', 'Masa por Pliego Prensa', 'Cantidad_Buenas', 'Cantidad_Malas', 'Número_Pliegos','Masa por Pliego Almacén'], inplace=True)


    self.progress.emit(65)

    df_p_cajas = df.copy()
    df_p_cajas.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje', 'Despacho_Bodega'], axis=1, inplace=True)
    df_p_cajas = df_p_cajas[df_p_cajas["tk_code"].isin(['PCAJ FON G','PCAJ FON M','PCAJ FON P','PCAJ LAT G','PCAJ LAT P','PCAR N','PCAR TS',
    'PEGTT','PINS SISA','H TIR 4ESQ','H TIR FG','H TIR FM','H TIR FP','H TIR LG','H TIR LP','D TIR 2L','D TIR 4ESQ','D TIR FG','D TIR FM',
    'D TIR FP','D TIR LG','D TIR LP'])]


    df_p_cajas['Masa Salida Pegado Cajas (kg)'] = (df_p_cajas['Cantidad_Buenas'] * df_p_cajas['Peso_Ejemplar']) if len(df_p_cajas.index) > 0 else 0

    df_p_cajas['Masa Total Cajas (kg)'] = ((df_p_cajas['Cantidad_Buenas'] +df_p_cajas['Cantidad_Malas'])* df_p_cajas['Peso_Ejemplar']) if len(df_p_cajas.index) > 0 else 0

    df_p_cajas['Pérdida por Pegado de Cajas (kg)'] = (df_p_cajas['Cantidad_Malas'] * df_p_cajas['Peso_Ejemplar']) if len(df_p_cajas.index) > 0 else 0

    df_p_cajas['Unidades Totales'] = (df_p_cajas['Cantidad_Buenas'] + df_p_cajas['Cantidad_Malas']) if len(df_p_cajas.index) > 0 else 0

    df_p_cajas['Fracción de pérdida por Pegado de Cajas (%)'] = (df_p_cajas['Pérdida por Pegado de Cajas (kg)']/df_p_cajas['Masa Total Cajas (kg)']) if len(df_p_cajas.index) > 0 else 0

    df_p_cajas = df_p_cajas[['OP','Unidades Totales', 'Masa Salida Pegado Cajas (kg)', 'Pérdida por Pegado de Cajas (kg)', 'Fracción de pérdida por Pegado de Cajas (%)', 'Masa Total Cajas (kg)']]



    self.progress.emit(75)


    df_troquelado = df.copy()

    df_troquelado.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje', 'Despacho_Bodega'], axis=1, inplace=True)

    df_troquelado = df_troquelado[df_troquelado["tk_code"].isin(['TROPEQ TIR','TROMED TIR','TROGRD TIR','TROPLA TIR'])]

    df_troquelado = pd.merge(left= df_troquelado, right=df_datos_pre[['OP', 'Masa por Pliego Prensa']], how='left', left_on='OP', right_on = 'OP')

    df_troquelado['Masa de material conforme facturado (Kg)'] = df_troquelado['Qty'] * df_troquelado['Peso_Ejemplar']

    df_troquelado['Masa material para Troquelado(kg)'] = (df_troquelado['Cantidad_Buenas']+df_troquelado['Cantidad_Malas']) * df_troquelado['Masa por Pliego Prensa']

    df_troquelado['Pliegos para Troquelado'] = df_troquelado['Cantidad_Buenas'] + df_troquelado['Cantidad_Malas']

    df_troquelado = df_troquelado[['OP','Pliegos para Troquelado','Masa material para Troquelado(kg)', 'Qty','Masa de material conforme facturado (Kg)']]


    df_consolidado_movimientos = pd.merge(left=df_impresion, right=df_troquelado, how='left', left_on='OP', right_on = 'OP')

    df_revision = df.copy()

    df_revision.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje', 'Despacho_Bodega','Qty'], axis=1, inplace=True)

    df_revision = df_revision[df_revision["tk_code"].isin(['REVISADO','REVISADO2','REVISADO3','REVISADO 4','REVISADO5'])]

    df_revision = df_revision.groupby(by="OP").sum()

    df_revision['Masa pérdida Revisión'] =  df_revision['Cantidad_Malas']*df_revision['Peso_Ejemplar'] if len(df_revision.index) > 0 else 0


    self.progress.emit(85)

    df_consolidado_movimientos = (pd.merge(left=df_consolidado_movimientos, right=df_p_cajas, how='left', left_on='OP', right_on = 'OP')) if len(df_p_cajas.index) > 0 else df_consolidado_movimientos

    def calculateMerma(row):
      if len(df_p_cajas.index) > 0:
        if(row['Masa Total Cajas (kg)']> 0 ):
          return row['Masa material para Troquelado(kg)'] - row['Masa Total Cajas (kg)']
        else:
          return row['Masa material para Troquelado(kg)'] - row['Masa de material conforme facturado (Kg)']
      else:
        return row['Masa material para Troquelado(kg)'] - row['Masa de material conforme facturado (Kg)']

    
    df_consolidado_movimientos = (pd.merge(left=df_consolidado_movimientos, right=df_revision, how='left', left_on='OP', right_on = 'OP')) if len(df_revision.index) > 0 else df_consolidado_movimientos
 
    df_consolidado_movimientos['Merma Limpieza Troquel (kg)'] = df_consolidado_movimientos.apply(calculateMerma, axis=1)

    df_consolidado_movimientos['Merma Limpieza Troquel (kg)'] = df_consolidado_movimientos['Merma Limpieza Troquel (kg)'] - df_consolidado_movimientos['Masa pérdida Revisión']

    df_consolidado_movimientos['Fracción de pérdida por limpieza de troquel (%)'] = (df_consolidado_movimientos['Merma Limpieza Troquel (kg)']/(df_consolidado_movimientos['Masa material para Troquelado(kg)']+ df_consolidado_movimientos['Merma Limpieza Troquel (kg)']))

    df_consolidado_movimientos['Pérdida por arreglo de Impresión (kg) 2'] = df_consolidado_movimientos['Material para Arreglos e Impresión (Kg)'] - df_consolidado_movimientos['Masa material para Troquelado(kg)']

    

    

    df_consolidado_movimientos['Pérdida por  revisión (kg)'] = df_consolidado_movimientos['Masa pérdida Revisión']

    df_tipoProd = df.copy()

    df_tipoProd.drop_duplicates(subset=['OP'], inplace=True)

    df_tipoProd = df_tipoProd[['OP','titulo','Tipo Producto','Categoría FSC','Nota']]

    df_consolidado_movimientos = pd.merge(df_consolidado_movimientos, right=df_tipoProd, how='inner', on='OP')


    df_consolidado_movimientos = (df_consolidado_movimientos[['OP','titulo','Tipo Producto','Categoría FSC', 'Despacho_Bodega', 'Despachos de pliego almacén (Kg)', 'Pérdida Corte Inicial por exceso (Kg)', 'Fraccupib de pérdida por Corte Inicial (%)','Pliegos para Arreglo e Impresión', 'Material para Arreglos e Impresión (Kg)', 
    'Pérdida por arreglo de Impresión (Kg)','Pérdida por arreglo de Impresión (kg) 2', 'Fracción de pérdida por Impresión (%)', 'Pliegos para Troquelado','Masa material para Troquelado(kg)', 'Merma Limpieza Troquel (kg)','Fracción de pérdida por limpieza de troquel (%)','Unidades Totales','Masa Salida Pegado Cajas (kg)', 'Pérdida por Pegado de Cajas (kg)', 'Fracción de pérdida por Pegado de Cajas (%)'
    ,'Pérdida por  revisión (kg)','Qty','Masa de material conforme facturado (Kg)','Nota']]) if len(df_p_cajas.index) > 0 else (df_consolidado_movimientos[['OP','titulo','Tipo Producto','Categoría FSC', 'Despacho_Bodega', 'Despachos de pliego almacén (Kg)', 'Pérdida Corte Inicial por exceso (Kg)', 'Fraccupib de pérdida por Corte Inicial (%)','Pliegos para Arreglo e Impresión', 'Material para Arreglos e Impresión (Kg)', 
    'Pérdida por arreglo de Impresión (Kg)','Pérdida por arreglo de Impresión (kg) 2', 'Fracción de pérdida por Impresión (%)', 'Pliegos para Troquelado','Masa material para Troquelado(kg)', 'Merma Limpieza Troquel (kg)','Fracción de pérdida por limpieza de troquel (%)'
    ,'Pérdida por  revisión (kg)','Qty','Masa de material conforme facturado (Kg)','Nota']])

    

    df_consolidado_movimientos = df_consolidado_movimientos.rename(columns={"Despacho_Bodega":"Despacho de pliegos almacén",  'Qty':'Unidades Facturadas', 'Unidades Totales':'Unidades Totales para pegue','Nota':'Nota Reimpresión'})

    df_consolidado_movimientos['Fracción Material Conforme %'] = df_consolidado_movimientos['Masa de material conforme facturado (Kg)'] / df_consolidado_movimientos['Despachos de pliego almacén (Kg)']
    
    df_consolidado_movimientos.fillna(0.0,inplace=True)

    

    _path = os.path.join(path if len(path)> 0 else os.getcwd(), "movimientos_FSC_{}.xlsx".format(datetime.now().strftime("%Y%m%d-%H%M%S")))

    _path = _path.replace('/','//') if len(path) > 0 else _path.replace('\\','//')

    ops = df_consolidado_movimientos["OP"].to_list()


    df_trazabilidad = trazabilidad(self, ops, db_connection)

    with pd.ExcelWriter(_path, engine='xlsxwriter') as writer:

      workbook = writer.book
      
      worksheet = workbook.add_worksheet('Datos')
    
      cell_format = workbook.add_format()
      cell_format.set_text_wrap()
      cell_format.set_bg_color('#4BACC6')

      cell_format2 = workbook.add_format({'bg_color':'#9BBB59'})

      cell_format2.set_text_wrap()

      row_format = workbook.add_format()
      row_format.set_num_format('0.0%')
      row_format.set_bg_color('#b1ca7d')

      row_format1 = workbook.add_format()
      row_format1.set_num_format('#,##0.00')
      row_format1.set_text_wrap()

      (max_row, max_col) = df_consolidado_movimientos.shape

      column_settings = [{'header': column, 'header_format': cell_format if "Fracción" not in column else cell_format2, 'format':row_format if "Fracción" in column else row_format1} for column in df_consolidado_movimientos.columns]


      worksheet.add_table(0, 0, max_row, max_col-1, {'data':df_consolidado_movimientos.values.tolist(),'style':None,'columns': column_settings})

      worksheet.set_column(0, max_col-1, 12)

      if df_trazabilidad is not None:
        worksheet2 = workbook.add_worksheet('Trazabilidad')

        (max_row2, max_col2) = df_trazabilidad.shape

        cell_format4 = workbook.add_format()
        cell_format4.set_num_format('d/mm/yyyy h:mm')

        cell_format.set_bold()

        column_settings2 = [{'header': column, 'header_format': cell_format,'format': row_format1 if "Fecha" not in column else cell_format4} for column in df_trazabilidad.columns]

        worksheet2.add_table(0,0,max_row2, max_col2-1, {'data': df_trazabilidad.values.tolist(), 'style':None, 'columns': column_settings2})
        worksheet2.set_column(0, max_col2-1,18)

    self.message.emit("Reporte Generado!")
  
  except Exception as e:
    self.message.emit("Ha ocurrido un Error! Vuelva a Intentar, {}".format(e))
    return



