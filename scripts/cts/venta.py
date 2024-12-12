import pandas as pd 
from scripts.utils import utils

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

        # Eliminar filas innecesarias
        conceptos_to_drop = [
            #"Otros Impuestos",
            "Gastos Mercadeo Admon", 
            "Provisión de Inventario", 
            #"Otros Ingresos Operacionales",
        ]

        # Convertir las columnas a numéricas
        df_final['VALOR REAL'] = pd.to_numeric(df_final['VALOR REAL'], errors='coerce').fillna(0.0)
        df_final['VALOR PPTO'] = pd.to_numeric(df_final['VALOR PPTO'], errors='coerce').fillna(0.0)
        
        df_final = df_final[
            (~((df_final['VALOR REAL'] == 0.0) & (df_final['VALOR PPTO'] == 0.0))) &
            (df_final['CANAL'] != 'Resultado total') &
            (df_final['MARCA'] != 'Resultado') &
            (~df_final['CONCEPTO'].isin(conceptos_to_drop))
        ]
        
        # Convertir todo a mayusculas
        df_final = df_final.map(lambda x: x.upper() if isinstance(x, str) else x)
    
        # Crear la base de datos
        # return hoja_2024(df_final, año, mes, mes_largo)
        print(df_final)
    else:
        return 'BAD'
