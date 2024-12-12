
from datetime import datetime
import os, pandas as pd, numpy as np
from scripts.utils import utils
from .bd_directos import get_bd_directos
from .bd_indirectos import get_bd_indirectos

def extract_data(raw_data: dict, año, mes, mes_largo) -> None:

    # Lista para almacenar los datos del eje del cubo
    axis_records = []

    # Extraer toda la información del eje del cubo
    for axis in raw_data['E_AXIS_DATA']:
        axis_name = axis['AXIS']
        for record in axis['SET']:
                axis_data = {
                'AXIS': axis_name,
                'TUPLE_ORDINAL': record['TUPLE_ORDINAL'],
                'CHANM': record['CHANM'],
                'CAPTION': record['CAPTION'],
                'CHAVL': record['CHAVL'],
                }
                axis_records.append(axis_data)
    df_axis = pd.DataFrame(axis_records)

    # Lista para almacenar los datos de la celda del cubo
    cell_records = []

    # Extraer la información de la celda del cubo
    for record in raw_data['E_CELL_DATA']:
        cell_data = {
            'CELL_ORDINAL': record['CELL_ORDINAL'],
            'VALUE': record['VALUE'],
        }
        cell_records.append(cell_data)
    df_cell = pd.DataFrame(cell_records)

    # Organizar la informacion recopilada
    return sort_data(df_axis, df_cell, año, mes, mes_largo)

def sort_data(df_axis: pd.DataFrame, df_cell: pd.DataFrame, año, mes, mes_largo) -> None:
    if isinstance(df_axis, pd.DataFrame) & isinstance(df_cell, pd.DataFrame):

        # Cambiar la direccion de los ejes necesarios
        df_axis_chavl = df_axis.pivot(index='TUPLE_ORDINAL', columns='CHANM', values='CHAVL')
        df_axis_chavl.reset_index(inplace=True)
        df_axis_caption = df_axis.pivot(index='TUPLE_ORDINAL', columns='CHANM', values='CAPTION')
        df_axis_caption.reset_index(inplace=True)

        df_axis = pd.merge(df_axis_chavl, df_axis_caption, left_index=True, right_index=True)

        # Agrupamos cada tres filas las celdas usando el índice
        df_cell['Group'] = df_cell.index // 3
        df_valores = pd.DataFrame({
            'VALOR REAL': df_cell.groupby('Group')['VALUE'].nth(0).values, 
            'VALOR PPTO': df_cell.groupby('Group')['VALUE'].nth(1).values,  
            'EJECUCION': df_cell.groupby('Group')['VALUE'].nth(2).values, 
        })

        # Combinar los DataFrames
        df_final = pd.merge(df_axis, df_valores, left_index=True, right_index=True)

        # Eliminar las columnas y filas innecesarias
        columns_to_drop = [
            'TUPLE_ORDINAL_y',
            'TUPLE_ORDINAL_x',
            '4F3RL7LMA3OT6LD25GQCJ6RLP_x', 
            '0FISCPER3_x',
            '4F3RL7LMA3OT6LD25GQCJ6RLP_y'            
        ]
        df_final = df_final.drop(columns=columns_to_drop)
    
        # Renombrar las columnas
        columns_to_rename = {
            '0FISCPER3_y': 'PERIODO',
            '0COSTCENTER_x': 'CECO',
            '0COSTCENTER_y':'NOMBRE CECO',
            '0COSTELMNT_x': 'CLASE DE COSTO',
            '0COSTELMNT_y':'CUENTA'
        }
        df_final = df_final.rename(columns=columns_to_rename)

        # Eliminar valores innecesarios en registros espesificos
        df_final['CECO'] = df_final['CECO'].str.replace('PRBE00|PRBE', '', regex=True)
        df_final['CLASE DE COSTO'] = df_final['CLASE DE COSTO'].str.replace('PRBE', '', regex=False)
        df_final = df_final[(df_final['CUENTA'] != 'Resultado') &
            (df_final['NOMBRE CECO'] != 'Resultado')
        ]

        # Convertir las columnas a numéricas
        df_final['VALOR REAL'] = pd.to_numeric(df_final['VALOR REAL'], errors='coerce').fillna(0.0)
        df_final['VALOR PPTO'] = pd.to_numeric(df_final['VALOR PPTO'], errors='coerce').fillna(0.0)
        df_final['EJECUCION'] = pd.to_numeric(df_final['EJECUCION'], errors='coerce').fillna(0.0)
        
        # Reorganizar las columnas
        df_final = df_final[[
            'PERIODO', 'CECO', 'NOMBRE CECO', 'CLASE DE COSTO', 
            'CUENTA', 'VALOR REAL', 'VALOR PPTO', 'EJECUCION'
        ]]

        # Obtener el mes para extraer solo esa información
        fecha_mes = f"{mes}"
        meses = {
            '1': 'Ene', '2': 'Feb',
            '3': 'Mar', '4': 'Abr',
            '5': 'May', '6': 'Jun',
            '7': 'Jul', '8': 'Ago',
            '9': 'Sep', '10': 'Oct',
            '11': 'Nov', '12': 'Dic'
        }
        mes_letras = meses.get(str(fecha_mes), 'Mes inválido')
        df_final = df_final[df_final['PERIODO'] == mes_letras]

        # Convertir todo a mayusculas
        df_final = df_final.map(lambda x: x.upper() if isinstance(x, str) else x)

        # Crear la base de datos
        return hoja_bd_gastos(df_final, año, mes, mes_largo)
    
def hoja_bd_gastos(df_edg: pd.DataFrame, año, mes, mes_largo):

    try:
        # Ruta al directorio de usuario
        base_dir = os.path.expanduser('~')

        # Directorio a las bases de datos
        sub_dirs_mapping = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'MAPPING'
        ]

        # Ruta de cada base de datos
        pac = os.path.join(base_dir, *sub_dirs_mapping, 'pac.xlsx')
        personal = os.path.join(base_dir, *sub_dirs_mapping, 'personal.xlsx')
        otras_cuentas = os.path.join(base_dir, *sub_dirs_mapping, 'otras_cuentas.xlsx')
        ceco_concepto = os.path.join(base_dir, *sub_dirs_mapping, 'ceco_concepto.xlsx')

        # DataFrames de las bases de datos mapeadas
        df_pac = pd.read_excel(pac)
        df_personal = pd.read_excel(personal)
        df_otras_cuentas = pd.read_excel(otras_cuentas)
        df_ceco_concepto = pd.read_excel(ceco_concepto)

        # Agregar columna año
        año = datetime.now().year
        df_edg.insert(0, 'AÑO', año)

        # Agregar columnas calculadas

        # ================================ #
        # * OBTENER LA COLUMNA - 'MARCA' * #
        # ================================ #
        df_edg = pd.merge(df_edg,df_ceco_concepto[['CECO', 'MARCA']], on="CECO", how="left")

        # =================== =============== #
        # * OBTENER LA COLUMNA - 'CECOS BD' * #
        # =================================== #
        df_edg = pd.merge(df_edg, df_ceco_concepto[['CECO', 'CONCEPTO']], on='CECO', how='left')
        df_edg = df_edg.rename(columns={'CONCEPTO': 'CECOS BD'})

        # ============================== #
        # * OBTENER LA COLUMNA - 'PAC' * #
        # ============================== #

        df_pac['CENTRO DE COSTO'] = df_pac['CENTRO DE COSTO'].astype(str)
        df_pac['CLASE DE COSTO'] = df_pac['CLASE DE COSTO'].astype(str)
        
        df_edg = pd.merge(
            df_edg,
            df_pac[['CENTRO DE COSTO', 'CLASE DE COSTO', 'RUBRO']], 
            left_on=['CECO', 'CLASE DE COSTO'],
            right_on=['CENTRO DE COSTO', 'CLASE DE COSTO'], 
            how='left'
        )
        df_edg = df_edg.drop(columns=['CENTRO DE COSTO'])
        df_edg = df_edg.rename(columns={'RUBRO': 'PAC'})
        del df_pac

        # =================================== #
        # * OBTENER LA COLUMNA - 'PERSONAL' * #
        # =================================== #

        df_personal['CUENTA'] = df_personal['CUENTA'].astype(str)
        df_edg = pd.merge(
            df_edg,
            df_personal[['CUENTA', 'CONCEPTO']], 
            left_on='CLASE DE COSTO', 
            right_on='CUENTA', 
            how='left'
        )
        df_edg = df_edg.drop(columns=['CUENTA_y'])
        df_edg = df_edg.rename(columns={'CONCEPTO': 'PERSONAL', 'CUENTA_x': 'CUENTA'})

        # ================================== #
        # * OBTENER LA COLUMNA - 'NEGOCIO' * #
        # ================================== #
        df_edg = pd.merge(df_edg, df_ceco_concepto[['CECO', 'NEGOCIO']], on='CECO', how='left')

        # ================================== #
        # * OBTENER LA COLUMNA - 'CLIENTE' * #
        # ================================== #
        df_edg = pd.merge(df_edg, df_ceco_concepto[['CECO', 'CLIENTE']], on='CECO', how='left')

        # ================================ #
        # * OBTENER LA COLUMNA - 'OTRAS' * #
        # ================================ #

        # Condiciones necesarias
        condition1 = (df_edg['CECO'] == '11702200')
        condition2 = (df_edg['NEGOCIO'] == "FPT")
        condition3 = df_edg['CLASE DE COSTO'].isin(
            ['5235500000', '5240200000', '5230950000', '5235602505', '5295050000', 
            '5210350000', '5235951500', '5220950000', '5220101000', '5295952500', 
            '5210950500', '5220250000', '5260100500', '5210950500']
        )

        # Combinación de las condiciones
        combined_condition = condition1 & condition2 & condition3

        # Función para aplicar las reglas
        def asignar_otras(row, otras_cuentas_dict):
            if combined_condition.loc[row.name]:
                return row['CECOS BD']
            elif row['CECOS BD'] == "OTROS GASTOS VARIABLES":
                return row['CECOS BD']
            else:
                return otras_cuentas_dict.get(row['CLASE DE COSTO'], "")
        otras_cuentas_dict = df_otras_cuentas.set_index('CUENTA')['CONCEPTO'].to_dict()

        # Aplicar la función para crear la columna 'OTRAS'
        df_edg['OTRAS'] = df_edg.apply(lambda row: asignar_otras(row, otras_cuentas_dict), axis=1)

        # ================================ #
        # * OBTENER LA COLUMNA - 'CANAL' * #
        # ================================ #
        
        # Condiciones necesarias
        condition1 = (
            (df_edg['MARCA'] == "ARDEN FOR MEN") & 
            (df_edg['CECOS BD'] == "INVERSIÓN EN MERCADEO") & 
            (df_edg['PERSONAL'] == "GASTO DE PERSONAL")
        )
        
        condition2 = (
            (df_edg['MARCA'].isin(
            ["YARDLEY", "ACCESORIOS", "COVER GIRL", "ESSENCE", "CATRICE", "WELLA CONSUMO", 
            "OGX", "BIO OIL", "IMPORTADOS PROCTER", "MAX FACTOR", "NOPIKEX"])) & 
            (df_edg['CECOS BD'] == "INVERSIÓN EN MERCADEO") & (df_edg['PERSONAL'] == "GASTO DE PERSONAL")
        )

        condition3 =(
            (df_edg['MARCA'].isin(["WELLA PROFESSIONAL", "VITú"])) & 
            (df_edg['CECOS BD'] == "INVERSIÓN EN MERCADEO") & 
            (df_edg['PERSONAL'] == "GASTO DE PERSONAL")
        )
        
        df_edg['CANAL'] = df_edg.apply(lambda row: "TRADICIONAL" if condition1.loc[row.name] else (
            "MODERNO" if condition2.loc[row.name] else (
            "TIENDAS DE BELLEZA" if condition3.loc[row.name] else None)), axis=1)
        ceco_concepto_dict = df_ceco_concepto.set_index('CECO')['CANAL'].to_dict()
        df_edg['CANAL'] = df_edg.apply(lambda row: ceco_concepto_dict.get(row['CECO'], "") if pd.isna(row['CANAL']) else row['CANAL'], axis=1)
        df_edg['CANAL'] = df_edg['CANAL'].fillna('')

        # =================================== #
        # * OBTENER LA COLUMNA - 'CONCEPTO' * #
        # =================================== #

        conditions = [
            (df_edg['PAC'] == "PAC"), 
            (df_edg['OTRAS'] == "INVENTARIOS"),
            (df_edg['OTRAS'] == "REGALIAS"), 
            (df_edg['OTRAS'] == "DIF CAMBIO"),
            (df_edg['OTRAS'] == "DPP"), 
            (df_edg['OTRAS'] == "4X1000"),
            (df_edg['OTRAS'] == "AMORTIZACIÓN"), 
            (df_edg['OTRAS'] == "DONACIONES"),
            (df_edg['OTRAS'] == "NO OPERACIONALES"), 
            (df_edg['OTRAS'] == "CARTERA"),
            (df_edg['OTRAS'] == "OTROS"), 
            (df_edg['CECOS BD'] == "INVERSIÓN EN MERCADEO") & (df_edg['PERSONAL'] == "GASTO DE PERSONAL"),
            (df_edg['CECOS BD'] == "INVERSIÓN EN TRADE") & (df_edg['PERSONAL'] == "GASTO DE PERSONAL"),
            (df_edg['CECO'] == '11723000') & (df_edg['NEGOCIO'] == "FPT"),
            (df_edg['OTRAS'] == "GASTOS INVEST. Y DLLO"),
            (df_edg['OTRAS'] == "FLETE") & (df_edg['NEGOCIO'] == "FPT") & (~df_edg['CECOS BD'].isin(['TÉCNICO LEGAL'])),
            (df_edg['CECOS BD'] == "OTROS G. OPERACIONES") & (df_edg['NEGOCIO'] == "FPT"),
            (df_edg['CECOS BD'] == "GASTOS COMPRAS Y LOGÍSTICA") & (df_edg['NEGOCIO'] == "FPT") & (~df_edg['OTRAS'].isin(['FLETE', 'DIF CAMBIO'])),
            (df_edg['OTRAS'] == "OTROS GASTOS VARIABLES") & (df_edg['NEGOCIO'] == "FPT") & (~df_edg['CECOS BD'].isin(['TÉCNICO LEGAL', 'OTROS G. OPERACIONES', 'GASTOS COMPRAS Y LOGÍSTICA'])),
            (df_edg['CLASE DE COSTO'] == '5260100500') & (df_edg['NEGOCIO'] == "FPT") & (~df_edg['CECOS BD'].isin(['GASTOS INVEST. Y DLLO', 'OTROS G. OPERACIONES'])),
            (df_edg['OTRAS'] == "ICA")
            ]
        values = [
            "PAC", "INVENTARIOS", "REGALIAS", "DIF CAMBIO", "DPP", "4X1000", "AMORTIZACIÓN", "DONACIONES", 
            "NO OPERACIONALES", "CARTERA", "OTROS", "GASTOS DE PERSONAL (NIÑAS) MERCADEO", 
            "GASTOS DE PERSONAL (NIÑAS) TRADE", "TÉCNICO LEGAL",  df_edg['OTRAS'] ,"FLETE",
            "OTROS G. OPERACIONES", "GASTOS COMPRAS Y LOGÍSTICA", "OTROS GASTOS VARIABLES",
            "OTROS GASTOS VARIABLES", "ICA"
        ]
        df_edg['CONCEPTO'] = pd.Series([None] * len(df_edg))

        for condition, value in zip(conditions, values):
            df_edg.loc[condition, 'CONCEPTO'] = value
        df_edg['CONCEPTO'] = df_edg['CONCEPTO'].fillna(df_edg['CECOS BD'])

        # ============================= #
        # * EXPORTAR LA BASE DE DATOS * #
        # ============================= #

        if utils.export_db(df_edg, 'gastos', año, mes):
            bd_directos = get_bd_directos(df_edg, año, mes, mes_largo)
            bd_indirectos = get_bd_indirectos(df_edg, año, mes)
            bd_gastos = pd.concat([bd_directos, bd_indirectos], ignore_index=True)

            # Importar venta y costo y concatenarla
            sub_dirs_final_db_vyc = [
                'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
                'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
                'VENTA Y COSTO'
            ]
            path_bd_vyc = os.path.join(
                base_dir, *sub_dirs_final_db_vyc, f'bd_venta_y_costo_{mes}.{año}.xlsx'
            )
            bd_vyc = pd.read_excel(path_bd_vyc)

            # Cambiar la bd para concatenarla a la bd super final
            bd_vyc = bd_vyc.rename(columns={'CONCEPTO': 'DRIVER'})

            # Concatenar ambos cubos
            bd_final = pd.concat([bd_gastos, bd_vyc], ignore_index=True)

            # Añadir numero de mes
            mes_a_numero = {
                'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4, 'MAYO': 5,
                'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8, 'SEPTIEMBRE': 9,
                'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12
            }
            bd_final['# MES'] = bd_final['MES'].map(mes_a_numero)
            columnas = ['# MES'] + [col for col in bd_final.columns if col != '# MES']
            bd_final = bd_final[columnas]

            # Exportar la base de datos final
            sub_dirs_final_db = [
                'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
                'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL'
            ]
            sub_dirs_final_db_rep = [
                'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
                'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL', 'REPROCESOS'
            ] 

            bd_final_ppto = bd_final.drop(columns=['AÑO', 'VALOR REAL'])
            path_bd_final_ppto = os.path.join(
                base_dir, *sub_dirs_final_db_rep, f'DB_FINAL_{mes}.{año}_PPTO.xlsx'
            )

            if os.path.exists(path_bd_final_ppto):
                path_bd_final_ppto = os.path.join(
                    base_dir, *sub_dirs_final_db_rep, f'DB_FINAL_{mes}.{año}_REPROCESADA_PPTO.xlsx'
                )
                bd_final_ppto.to_excel(path_bd_final_ppto, index=False, sheet_name='Sheet1')
            else:
                path_bd_final_ppto = os.path.join(
                    base_dir, *sub_dirs_final_db, f'DB_FINAL_{mes}.{año}_PPTO.xlsx'
                )
                bd_final_ppto.to_excel(path_bd_final_ppto, index=False, sheet_name='Sheet1')
            
            bd_final_real = bd_final.drop(columns=['AÑO', 'VALOR PPTO'])
            path_bd_final_real = os.path.join(
                base_dir, *sub_dirs_final_db, f'DB_FINAL_{mes}.{año}_REAL.xlsx'
            )

            if os.path.exists(path_bd_final_real):
                path_bd_final_real = os.path.join(
                    base_dir, *sub_dirs_final_db_rep, f'DB_FINAL_{mes}.{año}_REPROCESADA_REAL.xlsx'
                )
                bd_final_real.to_excel(path_bd_final_real, index=False, sheet_name='Sheet1')
            else:
                path_bd_final_real = os.path.join(
                    base_dir, *sub_dirs_final_db, f'DB_FINAL_{mes}.{año}_REAL.xlsx'
                )
                bd_final_real.to_excel(path_bd_final_real, index=False, sheet_name='Sheet1')

            return True
        else:
            return False
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return False



