# Python Notebook - Resumen movimientos de Papel FSC

import pandas as pd

from sqlalchemy import create_engine, text

def movimientos(args):
  db_connection_str = 'mysql+pymysql://reports:cognos@192.168.1.238/mayaprin?charset=utf8'

  db_connection = create_engine(db_connection_str)

  query_str1 = """SELECT
  Tabla1.*,
  Dist.dr_quantity,
  Dist.dr_per_level_1,
  Dist.dr_per_level_2,
  Dist.dr_per_level_3,
  Dist.dr_kg_empty_level_2,
  Dist.dr_kg_empty_level_3,
  Dist.dr_weight
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
      jb.j_ucode3 AS Peso_Ejemplar
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
  ) AS Tabla1
  INNER JOIN (
    SELECT
      j.j_number,
      drv.dr_quantity,
      drv.dr_per_level_1,
      drv.dr_per_level_2,
      drv.dr_per_level_3,
      drv.dr_kg_empty_level_2,
      drv.dr_kg_empty_level_3,
      drv.dr_weight
    FROM
      job200 j
      INNER JOIN wo200 ON j.j_number = wo200.wo_job
      INNER JOIN delreq_task_view drv ON wo200.wo_number = drv.dr_wonum
  ) AS Dist ON Tabla1.j_number = Dist.j_number;""".format(startdate=args[0], endate= args[1] if len(args) > 1 else args[0])


  query_str2 = """SELECT
  Tabla1.*,
  Dist.dr_quantity,
  Dist.dr_per_level_1,
  Dist.dr_per_level_2,
  Dist.dr_per_level_3,
  Dist.dr_kg_empty_level_2,
  Dist.dr_kg_empty_level_3,
  Dist.dr_weight
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
      jb.j_ucode3 AS Peso_Ejemplar
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
  ) AS Tabla1
  INNER JOIN (
    SELECT
      j.j_number,
      drv.dr_quantity,
      drv.dr_per_level_1,
      drv.dr_per_level_2,
      drv.dr_per_level_3,
      drv.dr_kg_empty_level_2,
      drv.dr_kg_empty_level_3,
      drv.dr_weight
    FROM
      job200 j
      INNER JOIN wo200 ON j.j_number = wo200.wo_job
      INNER JOIN delreq_task_view drv ON wo200.wo_number = drv.dr_wonum
  ) AS Dist ON Tabla1.j_number = Dist.j_number;""".format(_list=args)

  query_str = query_str1 if type(args) is list else query_str2
  
  df = pd.read_sql_query(text(query_str), con = db_connection)
  print(df)
  '''df.Datos_Papel.fillna("", inplace=True)
  df.fillna(0.0,inplace=True)
  df["Peso_Ejemplar"] = df["Peso_Ejemplar"]/1000
  dfTransform = df.copy()
  dfTransform.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje','dr_quantity', 'dr_per_level_1', 'dr_per_level_2', 'dr_per_level_3', 'dr_kg_empty_level_2',
  'dr_kg_empty_level_3', 'dr_weight', 'Peso_Ejemplar'], axis=1, inplace=True)
  dfDimensions = df.copy()
  dfDimensions.drop(['Cantidad_Buenas', 'Cantidad_Malas','tk_code','Despacho_Bodega', 'dr_quantity', 'dr_per_level_1', 'dr_per_level_2', 'dr_per_level_3', 'dr_kg_empty_level_2',
  'dr_kg_empty_level_3', 'dr_weight', 'Peso_Ejemplar'], axis=1, inplace=True)

  dfDimensions[["Dimensiones Prensa"]] = dfDimensions.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[0]
  dfDimensions[["Número_Pliegos"]] = pd.to_numeric(dfDimensions.Datos_Papel.str.split(":", expand=True)[2].str.strip().str.split(",", expand=True)[1].str.split(" ", expand=True)[2])
  dfDimensions.drop(columns=["Datos_Papel"], inplace=True)
  pd.set_option('display.max_colwidth', None)

  dfDimensions

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


  dfMergedT = pd.merge(left=dfIniciales, right=dfPrensas, how="inner", left_on="j_number", right_on="j_number")


  dfMergedT.insert(8,"Material Impresión (Kg)", dfMergedT['Despachos de Bodega (Kg)'] - dfMergedT['Merma Corte Inicial (Kg)'])
  dfMergedT['Cantidad_Malas (Kg)'] = dfMergedT['Cantidad_Malas'] * dfMergedT['Masa por Pliego Prensa']


  dfMergedT['Fracción pérdida Impresión'] = dfMergedT['Cantidad_Malas (Kg)'] / dfMergedT['Material Impresión (Kg)']
  dfMergedT.drop(columns=['Cantidad_Total', 'Masa por Pliego Prensa', 'Cantidad_Buenas', 'Cantidad_Malas'], inplace=True)


  dfDistribucion = df.copy()
  dfDistribucion.drop(columns=['tk_code', 'Cantidad_Buenas', 'Cantidad_Malas', 'Datos_Papel', 'Despacho_Bodega', 'Ancho', 'Alto', 'Gramaje', 'dr_per_level_1','dr_per_level_2', 'dr_per_level_3',
  'dr_kg_empty_level_2','dr_kg_empty_level_3', 'dr_weight'], inplace=True)





  #dfDistribucion['Masa prod. Terminado'] = dfDistribucion.dr_weight - (dfDistribucion.dr_kg_empty_level_3 * dfDistribucion['no. Pallets']) - (dfDistribucion.dr_kg_empty_level_2 * dfDistribucion['no. Cajas'])
  dfDistribucion['Masa prod. Terminado'] = dfDistribucion.Peso_Ejemplar * dfDistribucion.dr_quantity


  dfDistribucion = dfDistribucion[['j_number', 'Masa prod. Terminado']]
  dfDistribucion

  dfDistribution = dfDistribucion.groupby('j_number').agg('sum')


  dfPCajas = df.copy()
  dfPCajas.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje','dr_quantity', 'dr_per_level_1', 'dr_per_level_2', 'dr_per_level_3', 'dr_kg_empty_level_2',
  'dr_kg_empty_level_3', 'dr_weight', 'Despacho_Bodega'], axis=1, inplace=True)
  dfPCajas = dfPCajas[dfPCajas["tk_code"].isin(['PCAJ FON G','PCAJ FON M','PCAJ FON P','PCAJ LAT G','PCAJ LAT P','PCAR N','PCAR TS',
  'PEGTT','PINS SISA','H TIR 4ESQ','H TIR FG','H TIR FM','H TIR FP','H TIR LG','H TIR LP','D TIR 2L','D TIR 4ESQ','D TIR FG','D TIR FM',
  'D TIR FP','D TIR LG','D TIR LP'])]

  dfPCajas['Salida Pegado Cajas (kg)'] = dfPCajas['Cantidad_Buenas'] * dfPCajas['Peso_Ejemplar']
  dfPCajas['Masa Total Cajas (kg)'] = (dfPCajas['Cantidad_Buenas'] +dfPCajas['Cantidad_Malas'])* dfPCajas['Peso_Ejemplar']
  dfPCajas['Merma Pegado Cajas (kg)'] = (dfPCajas['Cantidad_Malas'] * dfPCajas['Peso_Ejemplar'])
  dfPCajas['% Merma Pegado Cajas'] = dfPCajas['Merma Pegado Cajas (kg)']/dfPCajas['Masa Total Cajas (kg)']
  dfPCajas = dfPCajas[['j_number', 'Salida Pegado Cajas (kg)','Masa Total Cajas (kg)', 'Merma Pegado Cajas (kg)', '% Merma Pegado Cajas']]


  dfTroquel = df.copy()
  dfTroquel.drop(['Datos_Papel','Ancho', 'Alto', 'Gramaje','dr_quantity', 'dr_per_level_1', 'dr_per_level_2', 'dr_per_level_3', 'dr_kg_empty_level_2',
  'dr_kg_empty_level_3', 'dr_weight','Peso_Ejemplar', 'Despacho_Bodega'], axis=1, inplace=True)
  dfTroquel = dfTroquel[dfTroquel["tk_code"].isin(['TROPEQ TIR','TROMED TIR','TROGRD TIR','TROPLA TIR'])]

  dfTroquel = pd.merge(left= dfTroquel, right=dfPrensas[['j_number', 'Masa por Pliego Prensa']], how='left', left_on='j_number', right_on = 'j_number')

  dfTroquel['Masa Salida Troquel (kg)'] = dfTroquel['Cantidad_Buenas'] * dfTroquel['Masa por Pliego Prensa']
  dfTroquel = dfTroquel[['j_number', 'Masa Salida Troquel (kg)']]


  dfmovimientosMasa = pd.merge(left=dfMergedT, right=dfTroquel, how='left', left_on='j_number', right_on = 'j_number')



  dfmovimientosMasa = pd.merge(left=dfmovimientosMasa, right=dfPCajas, how='left', left_on='j_number', right_on = 'j_number')
  dfmovimientosMasa['Merma Limpieza Troquel (kg)'] = dfmovimientosMasa['Masa Salida Troquel (kg)'] - dfmovimientosMasa['Masa Total Cajas (kg)']

  dfmovimientosMasa = pd.merge(left=dfmovimientosMasa, right=dfDistribution,how='inner', left_on='j_number', right_on='j_number')
  dfmovimientosMasa['Merma Acabados finales (Kg)'] = dfmovimientosMasa['Salida Pegado Cajas (kg)'] - dfmovimientosMasa['Masa prod. Terminado']
  dfmovimientosMasa['Fracción pérdida Acabados finales'] = dfmovimientosMasa['Merma Acabados finales (Kg)'] / dfmovimientosMasa['Salida Pegado Cajas (kg)']'''




