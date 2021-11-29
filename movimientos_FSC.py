# Python Notebook - Resumen movimientos de Papel FSC

import pandas as pd

from sqlalchemy import create_engine, text

from datetime import datetime

import os

import math

from sqlalchemy.sql.expression import false

def movimientos(self,args,path):
  db_connection_str = 'mysql+pymysql://reports:cognos@192.168.1.238/pruebas?charset=utf8'

  db_connection = create_engine(db_connection_str)

  query_str1 = """SELECT
  Tabla1.*, Facturado.Qty
FROM
  (
    SELECT
      jb.j_number,
      tk_code,
      SUM(tr.wt_good_qty) AS Cantidad_Buenas,
      SUM(tr.wt_bad_qty) AS Cantidad_Malas,
      jb.j_special_ins AS Datos_Papel,
      cons_bodega.quantity AS Despacho_Bodega,
      cons_bodega.Ancho,
      cons_bodega.Alto,
      cons_bodega.Gramaje,
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
          stk.itm_fvals_2 AS "Gramaje"
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
        GROUP BY
          job200.j_number
      ) AS cons_bodega ON jb.j_number = cons_bodega.j_number
    WHERE
      j_status = "C"
      AND tsk.tk_code IN (
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
        'TROPLA TIR'
        
      )
      AND wt_source = 'TS'
      AND jb.j_ucode1 IS NOT NULL 
     AND jb.j_booked_in BETWEEN {startdate} AND {endate}
    GROUP BY
      jb.j_number,
      tsk.tk_code
  ) AS Tabla1 LEFT JOIN (
    SELECT ist.ist_job AS j_number, SUM(ist.ist_quantity) AS Qty
		FROM inv
		INNER JOIN ist ON 
		inv.inv_id = ist.ist_inv_id
		GROUP BY ist.ist_job
  ) AS Facturado ON Tabla1.j_number = Facturado.j_number;""".format(startdate=args[0], endate= args[1] if len(args) > 1 else args[0])


  query_str2 = """SELECT
  Tabla1.*, Facturado.Qty
FROM
  (
    SELECT
      jb.j_number,
      tk_code,
      SUM(tr.wt_good_qty) AS Cantidad_Buenas,
      SUM(tr.wt_bad_qty) AS Cantidad_Malas,
      jb.j_special_ins AS Datos_Papel,
      cons_bodega.quantity AS Despacho_Bodega,
      cons_bodega.Ancho,
      cons_bodega.Alto,
      cons_bodega.Gramaje,
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
          stk.itm_fvals_2 AS "Gramaje"
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
        GROUP BY
          job200.j_number
      ) AS cons_bodega ON jb.j_number = cons_bodega.j_number
    WHERE
      j_status = "C"
      AND tsk.tk_code IN (
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
        'TROPLA TIR'
        
      )
      AND wt_source = 'TS'
      AND jb.j_ucode1 IS NOT NULL 
      AND jb.j_number IN ({_list})
    GROUP BY
      jb.j_number,
      tsk.tk_code
  ) AS Tabla1 LEFT JOIN (
    SELECT ist.ist_job AS j_number, SUM(ist.ist_quantity) AS Qty
		FROM inv
		INNER JOIN ist ON 
		inv.inv_id = ist.ist_inv_id
		GROUP BY ist.ist_job
  ) AS Facturado ON Tabla1.j_number = Facturado.j_number;""".format(_list=args)

  query_str = query_str1 if type(args) is list else query_str2
  try:
    df = pd.read_sql_query(text(query_str), con = db_connection)
    self.progress.emit(25)

    if(len(df.index) == 0):
      self.message.emit("No se encontraron datos!")
      return

    df.Datos_Papel.fillna("", inplace=True)
    df.fillna(0.0,inplace=True)
    df["Peso_Ejemplar"] = pd.to_numeric(df["Peso_Ejemplar"])/1000

    dfTransform = df.copy()
    dfTransform.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje','Peso_Ejemplar', 'Qty'], axis=1, inplace=True)
    dfDimensions = df.copy()
    dfDimensions.drop(['Cantidad_Buenas', 'Cantidad_Malas','tk_code','Despacho_Bodega', 'Peso_Ejemplar', 'Qty'], axis=1, inplace=True)

    dfDimensions["Dimensiones Prensa"] = dfDimensions.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[0]
    dfDimensions["Número_Pliegos"] = pd.to_numeric(dfDimensions.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[1].str.split(" ", expand=True)[2])
    dfDimensions.drop(columns=["Datos_Papel"], inplace=True)
    pd.set_option('display.max_colwidth', None)

    self.progress.emit(30)

    dfTransformPress = dfTransform[dfTransform["tk_code"].isin(["PRE CX-TIR","PRE XL-TIR", "PRE SM-TIR", "PREIND TIR" ])]
    dfTransformPress

    def calcArea(x):
      dims  = [int(s)/1000 for s in str(x).split() if s.isdigit()]
      return pd.Series(dims, dtype='float64')
      
    dfDimensions[["Alto Prensa", "Ancho Prensa"]] = dfDimensions["Dimensiones Prensa"].apply(calcArea)
    dfDimensions.drop(columns=["Dimensiones Prensa"], inplace=True)

    dfDimensions.Ancho = dfDimensions.Ancho.transform(lambda x: x/1000)
    dfDimensions.Alto = dfDimensions.Alto.transform(lambda x: x/1000)
    dfDimensions.Gramaje = dfDimensions.Gramaje.transform(lambda x: x/1000)
    dfDimensions['Área Pliego Almacén'] = dfDimensions.Ancho * dfDimensions.Alto
    dfDimensions['Área Pliego Prensa'] = dfDimensions['Alto Prensa'] * dfDimensions['Ancho Prensa']
    dfDimensions.drop(columns=['Alto','Ancho', 'Alto Prensa', 'Ancho Prensa'], inplace=True)

    dfDimensions['Masa por Pliego Prensa'] = dfDimensions['Área Pliego Prensa']*dfDimensions.Gramaje
    dfDimensions['Masa por Pliego Almacén'] = dfDimensions['Área Pliego Almacén']*dfDimensions.Gramaje
    #dfDimensions.dropna(inplace=True)
    dfDimensions = dfDimensions.drop_duplicates(subset=['j_number'])

    dfTransformPress.insert(4, 'Cantidad_Total', dfTransformPress['Cantidad_Buenas']+dfTransformPress['Cantidad_Malas'])

    self.progress.emit(40)


    df_merged = pd.merge(left=dfTransformPress, right=dfDimensions, how='left', left_on='j_number', right_on='j_number')


    dfIniciales = df_merged.copy()

    dfIniciales = dfIniciales.drop_duplicates(subset=['j_number'])

    dfIniciales.drop(columns=['Masa por Pliego Prensa', 'Cantidad_Buenas', 'Cantidad_Malas', 'Cantidad_Total'], inplace=True)

    dfIniciales['Merma Corte Inicial (Kg)'] = (dfIniciales['Área Pliego Almacén'] - (dfIniciales['Área Pliego Prensa']*dfIniciales['Número_Pliegos'])) * dfIniciales['Gramaje']

    dfIniciales['Despachos de Bodega (Kg)'] = dfIniciales.Despacho_Bodega * dfIniciales['Masa por Pliego Almacén']

    dfIniciales = dfIniciales[['j_number','Despacho_Bodega','Despachos de Bodega (Kg)','Merma Corte Inicial (Kg)']]

    dfIniciales['Fracción Merma Corte Inicial'] = dfIniciales['Merma Corte Inicial (Kg)'] / dfIniciales['Despachos de Bodega (Kg)']

    dfPrensas = df_merged.copy()
    dfPrensas = dfPrensas.drop_duplicates(subset=['j_number'])
    dfPrensas.drop(columns=['Gramaje','tk_code','Área Pliego Almacén','Área Pliego Prensa', 'Masa por Pliego Almacén', 'Despacho_Bodega', 'Número_Pliegos'], inplace=True)


    self.progress.emit(50)

    dfMergedT = pd.merge(left=dfIniciales, right=dfPrensas, how="inner", left_on="j_number", right_on="j_number")


    dfMergedT.insert(8,"Material Impresión (Kg)", dfMergedT['Despachos de Bodega (Kg)'] - dfMergedT['Merma Corte Inicial (Kg)'])
    dfMergedT['Perdida Impresión (Kg)'] = dfMergedT['Cantidad_Malas'] * dfMergedT['Masa por Pliego Prensa']

    dfMergedT['Pliegos para Impresión'] = round(dfMergedT['Material Impresión (Kg)'] / dfMergedT['Masa por Pliego Prensa'],0)


    dfMergedT['Fracción pérdida Impresión'] = dfMergedT['Perdida Impresión (Kg)'] / dfMergedT['Material Impresión (Kg)']
    dfMergedT.drop(columns=['Cantidad_Total', 'Masa por Pliego Prensa', 'Cantidad_Buenas', 'Cantidad_Malas'], inplace=True)


    self.progress.emit(65)

    dfPCajas = df.copy()
    dfPCajas.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje', 'Despacho_Bodega'], axis=1, inplace=True)
    dfPCajas = dfPCajas[dfPCajas["tk_code"].isin(['PCAJ FON G','PCAJ FON M','PCAJ FON P','PCAJ LAT G','PCAJ LAT P','PCAR N','PCAR TS',
    'PEGTT','PINS SISA','H TIR 4ESQ','H TIR FG','H TIR FM','H TIR FP','H TIR LG','H TIR LP','D TIR 2L','D TIR 4ESQ','D TIR FG','D TIR FM',
    'D TIR FP','D TIR LG','D TIR LP'])]

    dfPCajas['Masa Salida Pegado Cajas (kg)'] = dfPCajas['Cantidad_Buenas'] * dfPCajas['Peso_Ejemplar']
    dfPCajas['Masa Total Cajas (kg)'] = (dfPCajas['Cantidad_Buenas'] +dfPCajas['Cantidad_Malas'])* dfPCajas['Peso_Ejemplar']
    dfPCajas['Merma Pegado Cajas (kg)'] = (dfPCajas['Cantidad_Malas'] * dfPCajas['Peso_Ejemplar'])
    dfPCajas['Unidades Totales'] = dfPCajas['Cantidad_Buenas'] + dfPCajas['Cantidad_Malas']
    dfPCajas['Fracción Merma Pegado Cajas'] = dfPCajas['Merma Pegado Cajas (kg)']/dfPCajas['Masa Total Cajas (kg)']
    dfPCajas = dfPCajas[['j_number','Unidades Totales', 'Masa Salida Pegado Cajas (kg)', 'Merma Pegado Cajas (kg)', 'Fracción Merma Pegado Cajas', 'Masa Total Cajas (kg)']]

    self.progress.emit(75)


    dfTroquel = df.copy()
    dfTroquel.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje', 'Despacho_Bodega'], axis=1, inplace=True)
    dfTroquel = dfTroquel[dfTroquel["tk_code"].isin(['TROPEQ TIR','TROMED TIR','TROGRD TIR','TROPLA TIR'])]

    dfTroquel = pd.merge(left= dfTroquel, right=dfPrensas[['j_number', 'Masa por Pliego Prensa']], how='left', left_on='j_number', right_on = 'j_number')

    dfTroquel['Masa de material conforme facturado (Kg)'] = dfTroquel['Qty'] * dfTroquel['Peso_Ejemplar']

    dfTroquel['Masa Salida Troquel (kg)'] = (dfTroquel['Cantidad_Buenas']+dfTroquel['Cantidad_Malas']) * dfTroquel['Masa por Pliego Prensa']

    dfTroquel['Pliegos para Troquelado'] = dfTroquel['Cantidad_Buenas'] + dfTroquel['Cantidad_Malas']

    dfTroquel = dfTroquel[['j_number','Pliegos para Troquelado','Masa Salida Troquel (kg)', 'Qty','Masa de material conforme facturado (Kg)']]


    dfmovimientosMasa = pd.merge(left=dfMergedT, right=dfTroquel, how='left', left_on='j_number', right_on = 'j_number')


    self.progress.emit(85)

    dfmovimientosMasa = pd.merge(left=dfmovimientosMasa, right=dfPCajas, how='left', left_on='j_number', right_on = 'j_number')
    dfmovimientosMasa['Merma Limpieza Troquel (kg)'] = dfmovimientosMasa['Masa Salida Troquel (kg)'] - dfmovimientosMasa['Masa Total Cajas (kg)']

    dfmovimientosMasa['Fracción de pérdida por limpieza de troquel (%)'] = dfmovimientosMasa['Merma Limpieza Troquel (kg)']/(dfmovimientosMasa['Masa Salida Troquel (kg)']+ dfmovimientosMasa['Merma Limpieza Troquel (kg)'])

    dfmovimientosMasa = dfmovimientosMasa[['j_number', 'Despacho_Bodega', 'Despachos de Bodega (Kg)', 'Merma Corte Inicial (Kg)', 'Fracción Merma Corte Inicial','Pliegos para Impresión', 'Material Impresión (Kg)', 
    'Perdida Impresión (Kg)', 'Fracción pérdida Impresión', 'Pliegos para Troquelado','Masa Salida Troquel (kg)', 'Merma Limpieza Troquel (kg)','Fracción de pérdida por limpieza de troquel (%)','Unidades Totales','Masa Salida Pegado Cajas (kg)', 'Merma Pegado Cajas (kg)', 'Fracción Merma Pegado Cajas'
  ,'Qty','Masa de material conforme facturado (Kg)']]

    dfmovimientosMasa = dfmovimientosMasa.rename(columns={"j_number":"OP", "Despacho_Bodega":"Despacho de pliegos almacén", "Masa Salida Troquel (k)":"Masa material troquelado conforme", "Despachos de Bodega (Kg)":"Despachos de pliego almacén (Kg)",
    "Merma Corte Inicial (Kg)":"Pérdida Corte Inicial por exceso (Kg)", "Fracción Merma Corte Inicial":"Fracción de pérdida por Corte Inicial (%)", "Material Impresión (Kg)":"Material para Impresión (Kg)",
    "Perdida Impresión (Kg)":"Pérdida por arreglo de  Impresión (Kg)","Fracción pérdida Impresión":"Fracción de pérdida por Impresión (%)", "Masa Salida Troquel (kg)":"Masa material para Troquelado(kg)",
    "Merma Limpieza Troquel (kg)":"Pérdida por Limpieza de Troquel (kg)","Merma Pegado Cajas (kg)":"Pérdida por  Pegado de Cajas (kg)", "Fracción Merma Pegado Cajas":"Fracción de pérdida por Pegado de Cajas"
    , 'Qty':'Unidades Facturadas'})
    
    dfmovimientosMasa.fillna(0.0,inplace=True)

    dfmovimientosMasa["Pérdida por  revisión (kg)"] = dfmovimientosMasa["Masa material para Troquelado(kg)"] - dfmovimientosMasa['Pérdida por Limpieza de Troquel (kg)'] -dfmovimientosMasa["Masa de material conforme facturado (Kg)"] - dfmovimientosMasa["Pérdida por  Pegado de Cajas (kg)"]

    _path = os.path.join(path if len(path)> 0 else os.getcwd(), "movimientos_FSC_{}.xlsx".format(datetime.now().strftime("%Y%m%d-%H%M%S")))

    _path = _path.replace('/','//') if len(path) > 0 else _path.replace('\\','//')

    with pd.ExcelWriter(_path, engine='xlsxwriter') as writer:
      dfmovimientosMasa.to_excel(writer, sheet_name="Datos", index = False)

      workbook = writer.book
      
      worksheet = writer.sheets['Datos']
    
      cell_format = workbook.add_format()
      cell_format.set_text_wrap()
      cell_format.set_bg_color('#ff7328')

      cell_format2 = workbook.add_format({'bg_color':'#10c458'})

      cell_format2.set_text_wrap()

      (max_row, max_col) = dfmovimientosMasa.shape

      column_settings = [{'header': column, 'header_format': cell_format if "Fracción" not in column else cell_format2} for column in dfmovimientosMasa.columns]


      worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

      # Make the columns wider for clarity.
      worksheet.set_column(0, max_col - 1, 12)
    
    self.message.emit("Reporte Generado!")
  
  except Exception as e:
    self.message.emit("Ha ocurrido un Error! Vuelva a Intentar, {}".format(e))
    return



