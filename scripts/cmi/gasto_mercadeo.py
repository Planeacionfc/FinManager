import pandas as pd 

def extract_data(raw_data: dict) -> None:

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
    return sort_data(df_axis, df_cell)

def sort_data(df_axis: pd.DataFrame, df_cell: pd.DataFrame) -> None:
    if isinstance(df_axis, pd.DataFrame) & isinstance(df_cell, pd.DataFrame):

        # Cambiar la direccion del eje 
        df_axis = df_axis.pivot(index='TUPLE_ORDINAL', columns='CHANM', values='CAPTION')
        df_axis.reset_index(inplace=True)

        # Agrupamos cada dos filas las celdas usando el índice
        df_cell['Group'] = df_cell.index // 2
        df_valores = pd.DataFrame({
            'VALOR REAL': df_cell.groupby('Group')['VALUE'].nth(0).values, 
            'VALOR PPTO': df_cell.groupby('Group')['VALUE'].nth(1).values,  
        })

        # Combinar los DataFrames 
        df_final = pd.merge(df_axis, df_valores, left_index=True, right_index=True)

        # Eliminar las columnas y filas innecesarias
        columns_to_drop = [
            'TUPLE_ORDINAL',
            '49Y91DLZTM1X6COYYC5BXNTX9', 
            '0SALESORG',
        ]
        df_final = df_final.drop(columns=columns_to_drop)
    
        # Renombrar las columnas
        columns_to_rename = {
            '0DISTR_CHAN': 'CANAL',
            'ZCH_MARCA': 'MARCA',
            '4ACUF4BK1L8BRGU0XQAGL7PZ1': 'CONCEPTO',
        }
        df_final = df_final.rename(columns=columns_to_rename)

        # Convertir las columnas a numéricas
        df_final['VALOR REAL'] = pd.to_numeric(df_final['VALOR REAL'], errors='coerce').fillna(0.0)
        df_final['VALOR PPTO'] = pd.to_numeric(df_final['VALOR PPTO'], errors='coerce').fillna(0.0)
        
        # Eliminar filas inncesarias
        df_final = df_final[
            (~((df_final['VALOR REAL'] == 0.0) & (df_final['VALOR PPTO'] == 0.0))) &
            (df_final['CONCEPTO'] == 'Gastos Mercadeo Admon') &
            (df_final['MARCA'] != 'Resultado')]
        
        # Convertir todo a mayusculas
        df_final = df_final.map(lambda x: x.upper() if isinstance(x, str) else x)
    
        # Crear la base de datos
        return hoja_g_mercadeo(df_final)
    else:
        import traceback
        print(f"Ha ocurrido un error:")
        traceback.print_exc()
        return False

def hoja_g_mercadeo(df_merc_admon: pd.DataFrame):
    try:
        # Mapear y reemplazar marcas
        mapa_marcas = {
            'BEAUTYHOLICS': 'OTRAS MP',
            'PREMIOS DE MOTIVACIÓN VENTA DIRECTA': 'OTRAS MP',

            'DOVE': 'OTROS EXP. NO LOCALES', 'IMPORTADOS PROCTER': 'OTROS EXP. NO LOCALES',
            'OTROS': 'OTROS EXP. NO LOCALES', #'REVOX': 'OTROS EXP. NO LOCALES',
            'SALLY HANSEN': 'OTROS EXP. NO LOCALES', 'ST. IVES': 'OTROS EXP. NO LOCALES',
            'UTOPICK': 'OTROS EXP. NO LOCALES', 'DAVINES': 'OTROS EXP. NO LOCALES',

            'FAMILIA': 'OTROS EXP. LOCALES',
            'LOREAL': 'OTROS EXP. LOCALES', 'OTROS (CANAL FPT)': 'OTROS EXP. LOCALES',

            'AMAZON': 'OTROS CLIENTES DO',
            'CALA': 'OTROS CLIENTES DO',

            'MAX FACTOR EXPORTACIONES': 'MAX FACTOR', 
            'VITÚ EXPORTACIONES': 'VITÚ', 
            'MAUI': 'OGX', 'CONNECT': 'ARDEN FOR MEN', 
            'NUDE EXPORTACIÓN': 'NUDE',
            'BALANCE': 'HENKEL'
        }
        df_merc_admon['MARCA'] = df_merc_admon['MARCA'].replace(mapa_marcas)

        # Mapear y reemplazar canales
        mapa_canales = {
            'PUNTOS DE VENTA': 'ALMACENES',
            'BAZAR VENTA DIRECTA': 'ALMACENES'
        }
        df_merc_admon['CANAL'] = df_merc_admon['CANAL'].replace(mapa_canales)
        df_merc_admon.loc[(df_merc_admon['CANAL'] == 'SIN ASIGNAR') & (df_merc_admon['MARCA'] != 'SIN ASIGNAR'), 'CANAL'] = 'OTROS'
        return df_merc_admon
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return False
    