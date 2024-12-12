import pandas as pd 
from scripts.utils import utils
from controller.cmiController import cmiController

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
        return hoja_2024(df_final, año, mes, mes_largo)
    else:
        return 'BAD'

def hoja_2024(df_2024: pd.DataFrame, año, mes, mes_largo):
    try:
        
        # Mapear y reemplazar marcas
        mapa_marcas = {
            'BEAUTYHOLICS': 'OTRAS MP',
            'PREMIOS DE MOTIVACIÓN VENTA DIRECTA': 'OTRAS MP',

            'DOVE': 'OTROS EXP. NO LOCALES', 'IMPORTADOS PROCTER': 'OTROS EXP. NO LOCALES',
            'OTROS': 'OTROS EXP. NO LOCALES', #'REVOX': 'OTROS EXP. NO LOCALES',
            'SALLY HANSEN': 'OTROS EXP. NO LOCALES', 'ST. IVES': 'OTROS EXP. NO LOCALES',
            'UTOPICK': 'OTROS EXP. NO LOCALES', #'DAVINES': 'OTROS EXP. NO LOCALES',

            'FAMILIA': 'OTROS EXP. LOCALES',
            'LOREAL': 'OTROS EXP. LOCALES', 'OTROS (CANAL FPT)': 'OTROS EXP. LOCALES',

            'AMAZON': 'OTROS CLIENTES DO',
            'CALA': 'OTROS CLIENTES DO',
            'LA FABRIL': 'OTROS CLIENTES DO',
            'WORMSER': 'USA',

            'MAX FACTOR EXPORTACIONES': 'MAX FACTOR', 
            'VITÚ EXPORTACIONES': 'VITÚ', 
            'MAUI': 'OGX', 'CONNECT': 'ARDEN FOR MEN', 
            'NUDE EXPORTACIÓN': 'NUDE',
            'BALANCE': 'HENKEL'
        }
        df_2024['MARCA'] = df_2024['MARCA'].replace(mapa_marcas)

        # Mapear y reemplazar canales
        mapa_canales = {
            'PUNTOS DE VENTA': 'ALMACENES',
            'BAZAR VENTA DIRECTA': 'ALMACENES',
            'BAZARES-FERIAS-2': 'ALMACENES'
        }
        df_2024['CANAL'] = df_2024['CANAL'].replace(mapa_canales)
        df_2024.loc[(df_2024['CANAL'] == 'SIN ASIGNAR') & (df_2024['MARCA'] != 'SIN ASIGNAR'), 'CANAL'] = 'OTROS'
        df_2024.loc[(df_2024['CANAL'] == 'FABRIC PARA TERCEROS') & (df_2024['MARCA'] == 'OTROS EXP. NO LOCALES'), 'MARCA'] = 'OTROS EXP. LOCALES'
        df_2024.loc[(df_2024['CANAL'] == 'FABRIC PARA TERCEROS') & (df_2024['MARCA'] == 'SIN ASIGNAR'), 'MARCA'] = 'OTROS EXP. LOCALES'

        # Mapear y agregar la columna segmentos
        mapa_segmento = {
        'OTROS CLIENTES DO': 'DEMAND OWNERS', 
        'MARKETING PERSONAL': 'DEMAND OWNERS',
        'OMNILIFE': 'DEMAND OWNERS', 'JERONIMO MARTINS': 'DEMAND OWNERS',
        'LEONISA': 'DEMAND OWNERS', 'LOCATEL': 'DEMAND OWNERS',
        'NOVAVENTA': 'DEMAND OWNERS', 'EL ÉXITO': 'DEMAND OWNERS',
        'MILAGROS ENTERPRISE': 'DEMAND OWNERS', 'LA POPULAR': 'DEMAND OWNERS',
        'D1': 'DEMAND OWNERS', 'USA': 'DEMAND OWNERS',
        'USA': 'DEMAND OWNERS',

        'UNILEVER': 'EXP. LOCALES', 'NATURA': 'EXP. LOCALES',
        'BIOTECNIK': 'EXP. LOCALES', 'NIVEA': 'EXP. LOCALES',
        'BRITO': 'EXP. LOCALES', 'AVON': 'EXP. LOCALES',
        'OTROS EXP. LOCALES': 'EXP. LOCALES', 'ALICORP': 'EXP. LOCALES',
        'SOLLA': 'EXP. LOCALES', 'ECAR': 'EXP. LOCALES',
        'FISA': 'EXP. LOCALES', 'KIMBERLY': 'EXP. LOCALES',
        'BELCORP': 'EXP. LOCALES', 'AMWAY': 'EXP. LOCALES',
        'PROCTER AND GAMBLE': 'EXP. LOCALES', 'HENKEL': 'EXP. LOCALES',
        'DIAL': 'EXP. LOCALES', 'BEIERSDORF': 'EXP. LOCALES',
        'OTROS EXP. LOCALES': 'EXP. LOCALES',

        'MAX FACTOR': 'EXP. NO LOCALES', 'DYCLASS': 'EXP. NO LOCALES',
        'WELLA CONSUMO': 'EXP. NO LOCALES', 'BIO OIL': 'EXP. NO LOCALES',
        'OGX': 'EXP. NO LOCALES', 'COVER GIRL': 'EXP. NO LOCALES',
        'WELLA PROFESSIONAL': 'EXP. NO LOCALES', 'ADIDAS': 'EXP. NO LOCALES',
        'ACCESORIOS': 'EXP. NO LOCALES', 'BURTS_BEES': 'EXP. NO LOCALES',
        'NOPIKEX': 'EXP. NO LOCALES', 'QVS': 'EXP. NO LOCALES',
        'UBU': 'EXP. NO LOCALES', 'ESSENCE': 'EXP. NO LOCALES',
        'MORROCCANOIL': 'EXP. NO LOCALES', 'HASK': 'EXP. NO LOCALES',
        'HERBAL ESSENCES': 'EXP. NO LOCALES', 
        'LOVE, BEAUTY AND PLANET': 'EXP. NO LOCALES',
        'CATRICE': 'EXP. NO LOCALES', 'NATURAL PARADISE': 'EXP. NO LOCALES',
        'OLAY': 'EXP. NO LOCALES', 'MID': 'EXP. NO LOCALES',
        'SECRET': 'EXP. NO LOCALES', 'FEBREZE': 'EXP. NO LOCALES',
        'TAMPAX': 'EXP. NO LOCALES', 'OFCORSS C.I HERMECO': 'EXP. NO LOCALES',
        'CADIVEU': 'EXP. NO LOCALES', 'MAX FACTOR GLOBAL': 'EXP. NO LOCALES',
        'SEBASTIAN': 'EXP. NO LOCALES',
        'AFFRESH': 'EXP. NO LOCALES', 'COSMETRIX': 'EXP. NO LOCALES',
        'INCENTIVOS MAX FACTOR': 'EXP. NO LOCALES', 
        'OTROS EXP. NO LOCALES': 'EXP. NO LOCALES',
        'DAVINES': 'EXP. NO LOCALES',
        'REVOX': 'EXP. NO LOCALES',

        'ARDEN FOR MEN': 'MARCAS PROPIAS', 'NUDE': 'MARCAS PROPIAS',
        'ELIZABETH ARDEN': 'MARCAS PROPIAS', 'YARDLEY': 'MARCAS PROPIAS',
        'VITÚ': 'MARCAS PROPIAS', 'PREBEL': 'MARCAS PROPIAS',
        'OTRAS MP': 'MARCAS PROPIAS',

        'BODY CLEAR': 'NO APLICA', 'OTRAS': 'NO APLICA',
        'GILLETTE': 'NO APLICA', 'L&G ASOCIADOS': 'NO APLICA',
        'CATÁLOGO DE PRODUCTOS': 'NO APLICA', 'HINODE': 'NO APLICA',
        'PFIZER': 'NO APLICA', 'CONTEXPORT DISNEY': 'NO APLICA',
        'SYSTEM PROFESSIONAL': 'NO APLICA', 'WELONDA': 'NO APLICA',
        'SIN ASIGNAR': 'NO APLICA'
        }

        df_2024['SEGMENTO'] = df_2024['MARCA'].map(mapa_segmento)
        df_2024 = df_2024[['CANAL','MARCA', 'SEGMENTO'] + [col for col in df_2024.columns if col not in ['CANAL', 'MARCA', 'SEGMENTO']]]

        # CONDICIONES
        df_2024.loc[(df_2024['SEGMENTO'] == 'DEMAND OWNERS'), 'CANAL'] = 'FABRIC PARA TERCEROS'
        df_2024.loc[(df_2024['SEGMENTO'] == 'EXP. LOCALES'), 'CANAL'] = 'FABRIC PARA TERCEROS'
        df_2024.loc[(df_2024['CANAL'] == 'FABRIC PARA TERCEROS') & (df_2024['SEGMENTO'] == 'MARCAS PROPIAS'), 'CANAL'] = 'OTROS'
        df_2024.loc[(df_2024['CANAL'] == 'FABRIC PARA TERCEROS') & (df_2024['SEGMENTO'] == 'EXP. NO LOCALES'), 'CANAL'] = 'OTROS'
        #df_2024.loc[(df_2024['CONCEPTO'] == 'OTROS INGRESOS OPERACIONALES'), 'CONCEPTO'] = 'INGRESO NETO'

        # Crear la nueva fila como un DataFrame de una sola fila
        nueva_fila = pd.DataFrame({
            'CANAL': ['OTROS'], 
            'MARCA': ['OTRAS MP'], 
            'SEGMENTO': ['MARCAS PROPIAS'], 
            'CONCEPTO': ['INGRESO NETO'], 
            'VALOR REAL': [0.1], 
            'VALOR PPTO': [0.1]
        })

        # Añadir la nueva fila al DataFrame df_2024
        df_2024 = pd.concat([df_2024, nueva_fila], ignore_index=True)

        # Eliminar las filas donde 'MARCA' sea 'SIN ASIGNAR' y 'CONCEPTO' sea 'OTROS IMPUESTOS'
        df_2024 = df_2024[~((df_2024['MARCA'] == 'SIN ASIGNAR') & (df_2024['CONCEPTO'] == 'OTROS IMPUESTOS'))]
        df_2024 = df_2024[~(df_2024['CANAL'] == 'TIENDAS PROPIAS')]

        #Eliminar costos dle periodo
        df_2024 = df_2024[~(df_2024['CONCEPTO'] == 'CMV COSTOS DEL PERIODO')]

        mapa_marcas = {
            'OTROS IMPUESTOS': '4X1000',
        }
        df_2024['CONCEPTO'] = df_2024['CONCEPTO'].replace(mapa_marcas)
        df_2024 = df_2024[~((df_2024['CANAL'] == 'FABRIC PARA TERCEROS') & (df_2024['CONCEPTO'] == '4X1000'))]

        # Traer la parte de costos del periodo
        df_costos_per = cmiController.process_cmi_costos_per(año, mes_largo)

        df_2024 = pd.concat([df_2024, df_costos_per], ignore_index=True)

        #  Mapear y agregar mes y año
        fecha = f"{mes}.{año}"
        meses = {
            '1': 'ENERO', '2': 'FEBRERO',
            '3': 'MARZO', '4': 'ABRIL',
            '5': 'MAYO', '6': 'JUNIO',
            '7': 'JULIO', '8': 'AGOSTO',
            '9': 'SEPTIEMBRE', '10': 'OCTUBRE',
            '11': 'NOVIEMBRE', '12': 'DICIEMBRE'
        }
        mes_codigo, año = fecha.split('.')
        mes_letras = meses[mes_codigo]
        df_2024.insert(0, 'AÑO', año)
        df_2024.insert(1, 'MES', mes_letras)

        return utils.export_db(df_2024, 'v_y_c', año, mes)
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return False
    