import pandas as pd, os

def hoja_pac(año, mes):
    try:

        # Obtener mes y año
        fecha = f'{mes}.{año}'
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

        # Ruta al directirio de la base de datos PAC 
        base_dir = os.path.expanduser('~')
        sub_dirs_final_db = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'PALNTILLAS BI']
        pac = os.path.join(base_dir, *sub_dirs_final_db, 'PAC_BI.xls')
        df_pac = pd.read_excel(pac, engine='xlrd', sheet_name='Historia de Ventas')

        # Ruta al directorio de los drivers pac
        base_dir = os.path.expanduser('~')
        sub_dirs_drivers = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'DRIVERS'
        ]
        drivers = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_pac.xlsx')
        df_drivers = pd.read_excel(drivers)

        # Eliminar columnas y filas innecesarias
        df_pac = df_pac.drop(columns=[
            '|', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 11'
        ]).dropna().reset_index(drop=True)

        # Cambiar el nombre de las columnas - convertir a mayusculas
        df_pac = df_pac.rename(columns={
            'Unnamed: 4': 'MES/AÑO', 'Unnamed: 5': 'CANAL',
            'Unnamed: 6': 'NIT', 'Unnamed: 7': 'MARCA',
            'Unnamed: 8': 'SUB MARCA', 'Unnamed: 13': 'GRUPO',
            'Unnamed: 9': 'MATERIAL', 'Unnamed: 10': 'DESC. MATERIAL',
            'Unnamed: 14': 'VENTAS COLOCACION', 'Unnamed: 12': 'CATEGORIA',
        })
        df_pac = df_pac.apply(lambda x: x.astype(str).str.upper() if x.dtype == 'object' else x)
        df_pac['VENTAS COLOCACION'] = df_pac['VENTAS COLOCACION'].astype(float)

        # Cambiar nombres de marcas
        mapa_marcas = {
            'MAX FACTOR EXPORTACIONES': 'MAX FACTOR', 
            'VITÚ EXPORTACIONES': 'VITÚ', 
            'MAUI': 'OGX', 'CONNECT': 'ARDEN FOR MEN', 
            'NUDE EXPORTACIÓN': 'NUDE',
            'BALANCE': 'HENKEL'
        }
        df_pac['MARCA'] = df_pac['MARCA'].replace(mapa_marcas)

        # Reemplazar los registros de la columna 'CATEGORIA'
        df_pac['CATEGORIA'] = df_pac['CATEGORIA'].replace('TRATAMIENTO', 'TRATAMIENTO FACIAL')

        # Agrupar el df_pac por NIT, MARCA y CATEGORIA y sumar las VENTAS
        df_suma_ventas = df_pac.groupby(['MARCA', 'NIT', 'CATEGORIA'])['VENTAS COLOCACION'].sum().reset_index()


        # Hacer un merge entre df_suma_ventas y df_drivers para obtener el porcentaje correspondiente
        df_merged = pd.merge(
            df_suma_ventas, 
            df_drivers[
                ['MARCA', 'NIT', 'CATEGORIA', 'SUB MARCA', 'EXHIBICIONES Y ESPACIOS', 'PUBLICACIONES E IMPRESOS' ,'OIPV']
            ], 
            on=['MARCA', 'NIT', 'CATEGORIA'], how='inner'
        )

        # Obtener el valor de cada PAC
        df_merged['PAC EXHIBICIONES'] = (df_merged['VENTAS COLOCACION'] * df_merged['EXHIBICIONES Y ESPACIOS']).fillna(0)
        df_merged['PAC PUBLICACIONES'] = (df_merged['VENTAS COLOCACION'] * df_merged['PUBLICACIONES E IMPRESOS']).fillna(0)
        df_merged['PAC OIPV'] = (df_merged['VENTAS COLOCACION'] * df_merged['OIPV']).fillna(0)

        # Filtrar el dataframe combinado para no mezclar valores
        df_merged = df_merged.fillna('')
        df_merged = df_merged.sort_values(by=['PAC EXHIBICIONES', 'PAC PUBLICACIONES', 'PAC OIPV'], ascending=False)

        # Agrupar el DataFrame por 'MARCA' y sumar las ventas y los valores PAC correspondientes
        df_pac_mapeo = df_merged.groupby('MARCA').agg({
            'VENTAS COLOCACION': 'sum', 
            'PAC EXHIBICIONES': 'sum',
            'PAC PUBLICACIONES': 'sum',
            'PAC OIPV': 'sum'
        }).reset_index()

        # Agregar columnas calculadas
        df_pac_mapeo['PAC'] = (df_pac_mapeo['PAC EXHIBICIONES'] + 
            df_pac_mapeo['PAC PUBLICACIONES'] + 
            df_pac_mapeo['PAC OIPV']
        ) / 1000000
        df_pac_mapeo['CANAL'] = 'MODERNO'
        df_pac_mapeo['DRIVER'] = 'PAC'
        df_pac_mapeo['VALOR REAL'] = df_pac_mapeo['PAC']
        df_pac_mapeo['PORCENTAJE PAC'] = (df_pac_mapeo['PAC']/(df_pac_mapeo['VENTAS COLOCACION']/1000000))

        # Leer el ppto de pac 
        sub_dirs_drivers = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'PAC PRESUPUESTO'
        ]
        pac_path = os.path.join(base_dir, *sub_dirs_drivers, f'PAC_PPTO_{año}.xlsx')
        df_pac_ppto = pd.read_excel(pac_path)

        # Combinar el df pac ocn el ppto para añadir el ppto
        df_pac_mapeo = pd.merge(df_pac_mapeo, df_pac_ppto, on='MARCA', how='left')

        df_pac_mapeo = df_pac_mapeo[df_pac_mapeo['MES'] == mes_letras]

        return df_pac_mapeo
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return None
    