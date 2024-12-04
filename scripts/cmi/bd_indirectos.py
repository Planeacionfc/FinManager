import pandas as pd
from scripts.utils import utils

def get_bd_indirectos(df_edg: pd.DataFrame, año, mes):
    return hoja_bd_indirectos(df_edg, año, mes)

def hoja_bd_indirectos(df_edg: pd.DataFrame, año, mes):
    try:

        import pandas as pd
        import os

        # Ruta a directorio de cada base de datos
        base_dir = os.path.expanduser('~')
        sub_dirs_final_vyc = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
            'VENTA Y COSTO'
        ]
        sub_dirs_drivers = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'DRIVERS'
        ]
        sub_dirs_flete = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'FLETE CORREO'
        ]

        # Ruta a cada base de datos externa
        vyc = os.path.join(base_dir, *sub_dirs_final_vyc, f'bd_venta_y_costo_{mes}.{año}.xlsx')
        drivers = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_indirectos.xlsx')
        drivers_ventas_canal = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_ventas_canal.xlsx')
        drivers_moderno = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_moderno.xlsx')
        flete_correo_path = os.path.join(base_dir, *sub_dirs_flete, f'flete_correo_{año}.xlsx')

        # Leer las bases de datos externas
        df_vyc = pd.read_excel(vyc, engine='openpyxl', sheet_name='Sheet1')
        df_drivers = pd.read_excel(drivers, sheet_name='Sheet1')
        df_drivers_ventas_canal = pd.read_excel(drivers_ventas_canal, sheet_name='Sheet1')
        df_drivers_moderno = pd.read_excel(drivers_moderno, sheet_name='Sheet1')
        df_flete_correo = pd.read_excel(flete_correo_path)

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

        # Segmentos
        segmentos = [
            'EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'
        ]

        # Crear dataframe con filtros necesarios
        df_vyc = df_vyc[['CANAL', 'MARCA', 'SEGMENTO', 'CONCEPTO', 'VALOR REAL', 'VALOR PPTO']]
        df_drivers_real = df_drivers[df_drivers['AÑO'] == int(año)]
        df_drivers_ppto = df_drivers[df_drivers['AÑO'] == f"{año} P"]
        df_flete_correo = df_flete_correo[df_flete_correo['MES'] == mes_letras]

        # Dataframe filtrados de venta y costo
        df_vyc_mp = df_vyc[(df_vyc['SEGMENTO'] == 'MARCAS PROPIAS')]
        df_vyc_enl = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. NO LOCALES')]
        df_vyc_el = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. LOCALES')]
        df_vyc_do = df_vyc[(df_vyc['SEGMENTO'] == 'DEMAND OWNERS')]

        # Esta funcion retorna un dataframe filtrado espesificamente por ceco
        def filtrar_edg(nombre_ceco):
            df = df_edg[
                (df_edg['NOMBRE CECO'] == nombre_ceco) &
                ((df_edg['CLASE DE COSTO'].astype(str).str.startswith('52')) | 
                (df_edg['CLASE DE COSTO'].astype(str).str.startswith('51'))) &
                (df_edg['CUENTA'] != 'OTROS IMPUESTOS')
            ]
            return df

        # Esta funcion retorna un dataframe filtrado espesificamente por ceco
        def filtrar_edg_gid(nombre_ceco):
            # Dataframe filtrado por el ceco
            df = df_edg[
                (df_edg['NOMBRE CECO'] == nombre_ceco) &
                ((df_edg['CLASE DE COSTO'] != '5235050000')) &
                ((df_edg['CLASE DE COSTO'].astype(str).str.startswith('52')) | 
                (df_edg['CLASE DE COSTO'].astype(str).str.startswith('53')) |
                (df_edg['CLASE DE COSTO'].astype(str).str.startswith('L')) |
                (df_edg['CLASE DE COSTO'].astype(str).str.startswith('S'))) &
                (df_edg['CUENTA'] != 'OTROS IMPUESTOS')
            ]
            return df

        # Esta funcion crea un df y opera un valor con un porcentaje espesifico
        def operar_porcj(df_ceco, porcentajes):
            total_valores = df_ceco[['VALOR REAL', 'VALOR PPTO']].sum()
            segmentos = [
                'EXP. NO LOCALES', 'MARCAS PROPIAS', 
                'EXP. LOCALES', 'DEMAND OWNERS'
            ]
            df_porcentajes = pd.DataFrame({
                'SEGMENTO': segmentos,
                'PORCENTAJE': porcentajes
            })
            df_porcentajes['VALOR REAL CALCULADO'] = (
                (total_valores['VALOR REAL'] * df_porcentajes['PORCENTAJE'])/1000000
            )
            df_porcentajes['VALOR PPTO CALCULADO'] = (
                (total_valores['VALOR PPTO'] * df_porcentajes['PORCENTAJE'])/1000000
            )
            return df_porcentajes
        
        # =========================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS COMPRAS Y LOGISTICA' * #
        # =========================================================== #

        # Lista de tuplas con el nombre del centro de costo (ceco) y los porcentajes
        cecos = [
            ('GERENCIA DE NEGOCIAC', [0, 0.17, 0.427, 0.403]),
            ('NEGOCIACIÓN MP', [0, 0.14, 0.435, 0.425]),
            ('NEGOCIACIÓN ME', [0, 0.17, 0.388, 0.442]),
            ('GESTIÓN PROVEEDORES', [0, 0.5, 0.249, 0.251]),
            ('GERENCIA LOGÍSTICA', [0.15, 0.25, 0.346, 0.254]),
            ('TÉCNICO LEGAL', [0, 0.842, 0.075, 0.083]),
            ('PVO', [0.33, 0.225, 0.189, 0.256]),
            ('COMERCIO EXT FPT', [0.25, 0.05, 0.554, 0.146]),
            ('PLANEACIÓN FPT', [0.20, 0.30, 0.331, 0.169]),
            ('PLANEACIÓN RETAIL', [0.20, 0.30, 0.331, 0.169]),
            ('COMPRAS FPT', [0.20, 0.18, 0.451, 0.169]),
            ('COMPRAS RETAIL', [0.20, 0.18, 0.451, 0.169]),
        ] 

        dfs = {}
        for ceco, porcentajes in cecos:
            df = filtrar_edg(ceco)
            dfs[ceco] = operar_porcj(df, porcentajes)

        df_bd_indirectos_g_compras_logistica = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])
        for key, df in dfs.items():
            df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == key].merge(df, on='SEGMENTO')

            df_merged_real['VALOR REAL'] = (
                df_merged_real['VALOR REAL CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            df_merged_real['VALOR PPTO'] = (
                df_merged_ppto['VALOR PPTO CALCULADO'] * df_merged_ppto['VALOR PORCENTAJE']
            )
            columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

            # Concatenar ambos cecos al dataframe principal 'OK'
            df_bd_indirectos_g_compras_logistica = pd.concat(
                [df_bd_indirectos_g_compras_logistica, 
                df_merged_real[columns_to_add]], 
                ignore_index=False
            )
        del df_merged_real, df_merged_ppto

        # FPT CHECK - Repartir Retail por canal
        df_valores_filtrados = df_vyc[
            (~df_vyc['CANAL'].isin(['FABRIC PARA TERCEROS', 'E-COMMERCE']))
            & (df_vyc['CONCEPTO'] == 'INGRESO NETO')
        ]

        # Obtener los valores por marca
        df_total_marca = df_valores_filtrados.groupby('MARCA')[
            ['VALOR REAL', 'VALOR PPTO']
        ].sum().reset_index()
        df_total_marca = df_total_marca.rename(columns={
            'VALOR REAL': 'SUMA REAL',
            'VALOR PPTO': 'SUMA PPTO' 
        })

        # Obtener los valores por marca canal
        df_valores_marca_canal = df_valores_filtrados.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_marca_canal = df_valores_marca_canal.rename(columns={
            'VALOR REAL': 'VALOR MARCA REAL',
            'VALOR PPTO': 'VALOR MARCA PPTO'
        })

        # Obtener el porcentaje real y ppto por marca y canal
        df_porcj_canal_marca = pd.merge(df_valores_marca_canal, df_total_marca, on='MARCA')
        df_porcj_canal_marca['PORCENTAJE REAL'] = (
            df_porcj_canal_marca['VALOR MARCA REAL'] / df_porcj_canal_marca['SUMA REAL']
        )
        df_porcj_canal_marca['PORCENTAJE PPTO'] = (
            df_porcj_canal_marca['VALOR MARCA PPTO'] / df_porcj_canal_marca['SUMA PPTO']
        )

        # Combinar los dataframe para operar 
        df_merged = pd.merge(
            df_bd_indirectos_g_compras_logistica[df_bd_indirectos_g_compras_logistica['CANAL'] != 'FABRIC PARA TERCEROS'],
            df_porcj_canal_marca, on='MARCA', how='left'
        )
        df_merged['VALOR FINAL REAL'] = df_merged['PORCENTAJE REAL']*df_merged['VALOR REAL']
        df_merged['VALOR FINAL PPTO'] = df_merged['PORCENTAJE PPTO']*df_merged['VALOR PPTO']
        df_merged = df_merged.drop(columns=['VALOR REAL', 'VALOR PPTO'])
        df_merged = df_merged.rename(columns={
            'CANAL_y': 'CANAL', 'VALOR FINAL REAL': 'VALOR REAL',
            'VALOR FINAL PPTO': 'VALOR PPTO'
        })
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

        # DATAFRAME FINAL DE OTROS G. OPERACIONES 'OK'
        df_bd_indirectos_g_compras_logistica =  df_bd_indirectos_g_compras_logistica[
            df_bd_indirectos_g_compras_logistica['CANAL'] == 'FABRIC PARA TERCEROS'
        ]
        df_bd_indirectos_g_compras_logistica = pd.concat(
        [df_bd_indirectos_g_compras_logistica, 
        df_merged[columns_to_add]], 
        ignore_index=False
        )
        df_bd_indirectos_g_compras_logistica['RUBRO'] = 'GASTOS COMPRAS Y LOGÍSTICA'

        # ====================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS INVEST. Y DLLO' * #
        # ====================================================== #

        # Lista de tuplas con el nombre del centro de costo (ceco) y los porcentajes
        cecos = [
            ('I&D PRODUCTOS-DLLO', [0.01, 0.41, 0.253, 0.327]),
            ('BANCO DE PRODUCTOS', [0, 0, 0, 1]),
        ]

        dfs = {}
        for ceco, porcentajes in cecos:
            df = filtrar_edg_gid(ceco)
            dfs[ceco] = operar_porcj(df, porcentajes)

        df_bd_indirectos_g_invest_dllo = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])
        for key, df in dfs.items():
            df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_real['VALOR REAL'] = (
                df_merged_real['VALOR REAL CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            df_merged_real['VALOR PPTO'] = (
                df_merged_ppto['VALOR PPTO CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

            # Concatenar ambos cecos al dataframe principal 'OK'
            df_bd_indirectos_g_invest_dllo = pd.concat(
                [df_bd_indirectos_g_invest_dllo, 
                df_merged_real[columns_to_add]], 
                ignore_index=False
            )
        del df_merged_real, df_merged_ppto

        # FPT CHECK - Repartir Retail por canal
        df_valores_filtrados = df_vyc[
        (~df_vyc['CANAL'].isin(['FABRIC PARA TERCEROS', 'E-COMMERCE']))
        & (df_vyc['CONCEPTO'] == 'INGRESO NETO')]

        # Obtener los valores por marca canal
        df_total_marca = df_valores_filtrados.groupby('MARCA')[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_total_marca = df_total_marca.rename(columns={'VALOR REAL': 'SUMA REAL TOTAL', 'VALOR PPTO': 'SUMA PPTO TOTAL'})
        df_valores_marca_canal = df_valores_filtrados.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_marca_canal = df_valores_marca_canal.rename(columns={'VALOR REAL': 'VALOR REAL MARCA', 'VALOR PPTO': 'VALOR PPTO MARCA'})

        # Obtener el porcentaje por marca y canal
        df_porcj_canal_marca = pd.merge(df_valores_marca_canal, df_total_marca, on='MARCA')
        df_porcj_canal_marca['PORCENTAJE REAL'] = (
            df_porcj_canal_marca['VALOR REAL MARCA'] / df_porcj_canal_marca['SUMA REAL TOTAL']
        )
        df_porcj_canal_marca['PORCENTAJE PPTO'] = (
            df_porcj_canal_marca['VALOR PPTO MARCA'] / df_porcj_canal_marca['SUMA PPTO TOTAL']
        )

        # Combinar los dataframe para operar 
        df_merged = pd.merge(
            df_bd_indirectos_g_invest_dllo[df_bd_indirectos_g_invest_dllo['CANAL'] != 'FABRIC PARA TERCEROS'],
            df_porcj_canal_marca, on='MARCA', how='left'
        )
        df_merged['VALOR REAL FINAL'] = df_merged['PORCENTAJE REAL']*df_merged['VALOR REAL']
        df_merged['VALOR PPTO FINAL'] = df_merged['PORCENTAJE PPTO']*df_merged['VALOR PPTO']

        df_merged = df_merged.drop(columns=['VALOR REAL', 'VALOR PPTO'])
        df_merged = df_merged.rename(columns={
            'CANAL_y': 'CANAL', 'VALOR REAL FINAL': 'VALOR REAL', 'VALOR PPTO FINAL': 'VALOR PPTO'
        })
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']


        # DATAFRAME FINAL DE OTROS G. OPERACIONES 'OK'
        df_bd_indirectos_g_invest_dllo =  df_bd_indirectos_g_invest_dllo[
            df_bd_indirectos_g_invest_dllo['CANAL'] == 'FABRIC PARA TERCEROS'
        ]
        df_bd_indirectos_g_invest_dllo = pd.concat(
        [df_bd_indirectos_g_invest_dllo, 
        df_merged[columns_to_add]], 
        ignore_index=False
        )
        df_bd_indirectos_g_invest_dllo['RUBRO'] = 'GASTOS INVEST. Y DLLO'

        # ====================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS DE OPERACIONES' * #
        # ====================================================== #

        # Filtrar ejecucion de gastos a 'Operaciones Nacional'
        df_operaciones = df_edg[
            (df_edg['CONCEPTO'] == 'DISNAL') &
            (df_edg['NOMBRE CECO'] == 'OPERACIONES NACIONAL')
        ]

        # Obtener los valores principales para gastos de operaciones real
        valor_operaciones_real = df_operaciones['VALOR REAL'].sum()
        valor_flete_correo_real = df_flete_correo['VALOR REAL'].sum()
        valor_resta_operaciones_real = (valor_operaciones_real - valor_flete_correo_real)/1000000

        # Obtener los valores principales para gastos de operaciones real
        valor_operaciones_ppto = df_operaciones['VALOR PPTO'].sum()
        valor_flete_correo_ppto = df_flete_correo['VALOR PPTO'].sum()
        valor_resta_operaciones_ppto = (valor_operaciones_ppto - valor_flete_correo_ppto)/1000000

        # Dataframes filtrados de venta y costo
        df_vyc_mp = df_vyc[(df_vyc['SEGMENTO'] == 'MARCAS PROPIAS')]
        df_vyc_enl = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. NO LOCALES')]

        # Porcentaje de cada segmento
        porcj_enl = 0.626431187165
        porcj_mp =  0.373568812834

        # Repartir la resta por cada segmento real y ppto
        valor_mp_real = porcj_mp*valor_resta_operaciones_real
        valor_enl_real = porcj_enl*valor_resta_operaciones_real
        valor_mp_ppto = porcj_mp*valor_resta_operaciones_ppto
        valor_enl_ppto = porcj_enl*valor_resta_operaciones_ppto

        # Valor de Ingreso Neto real y ppto en 1 variable
        valor_in_mp = df_vyc_mp[
            (df_vyc_mp['CONCEPTO'] == 'INGRESO NETO') & (df_vyc_mp['CANAL'] != 'E-COMMERCE')
        ][['VALOR REAL', 'VALOR PPTO']].sum()
        valor_in_enl = df_vyc_enl[
            (df_vyc_enl['CONCEPTO'] == 'INGRESO NETO') & (df_vyc_enl['CANAL'] != 'E-COMMERCE')
        ][['VALOR REAL', 'VALOR PPTO']].sum()

        # Porcentaje real y ppto para repartir por canal y marca
        porcj_enl_real = valor_enl_real/valor_in_enl['VALOR REAL']
        porcj_mp_real = valor_mp_real/valor_in_mp['VALOR REAL']
        porcj_enl_ppto = valor_enl_ppto/valor_in_enl['VALOR PPTO']
        porcj_mp_ppto = valor_mp_ppto/valor_in_mp['VALOR PPTO']

        # Dataframe con el valor de cada canal y marca de cada segmento
        df_totales_g_operaciones_mp = (
            df_vyc_mp[(df_vyc_mp['CONCEPTO'] == 'INGRESO NETO') & (df_vyc_mp['CANAL'] != 'E-COMMERCE')]
        ).groupby(['CANAL', 'MARCA'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_totales_g_operaciones_enl = (
            df_vyc_enl[(df_vyc_enl['CONCEPTO'] == 'INGRESO NETO') & (df_vyc_enl['CANAL'] != 'E-COMMERCE')]
        ).groupby(['CANAL', 'MARCA'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Operar el porcentaje por la venta real y ppto
        df_totales_g_operaciones_enl['VALOR REAL FINAL'] = df_totales_g_operaciones_enl['VALOR REAL']*porcj_enl_real
        df_totales_g_operaciones_mp['VALOR REAL FINAL'] = df_totales_g_operaciones_mp['VALOR REAL']*porcj_mp_real
        df_totales_g_operaciones_enl['VALOR PPTO FINAL'] = df_totales_g_operaciones_enl['VALOR PPTO']*porcj_enl_ppto
        df_totales_g_operaciones_mp['VALOR PPTO FINAL'] = df_totales_g_operaciones_mp['VALOR PPTO']*porcj_mp_ppto

        df_totales_g_operaciones = pd.concat(
            [df_totales_g_operaciones_enl, df_totales_g_operaciones_mp], ignore_index=True
        )

        # Filtrar ejecucion de gastos a 'Logistica E-commerce'
        df_logistica = df_edg[(df_edg['NOMBRE CECO'] == 'LOGISTICA E-COMMERCE')]
        df_logistica_flete = df_edg[
            (df_edg['NOMBRE CECO'] == 'LOGISTICA E-COMMERCE') & 
            (df_edg['CUENTA'] == '"TRANSP, FLETES Y AC')
        ]

        # Obtener los valores principales para gastos de operaciones comercial real y ppto
        valor_resta_operaciones_ecommerce_real = (
            df_logistica['VALOR REAL'].sum() - df_logistica_flete['VALOR REAL'].sum()
        )/1000000
        valor_resta_operaciones_ecommerce_ppto = (
            df_logistica['VALOR PPTO'].sum() - df_logistica_flete['VALOR PPTO'].sum()
        )/1000000

        # Obtener el porcentaje de marca canal en ecommerce
        df_vyc_ecommerce = df_vyc[df_vyc['CANAL'] == 'E-COMMERCE']
        suma_ecommerce = df_vyc_ecommerce[df_vyc_ecommerce['CONCEPTO'] == 'INGRESO NETO'][['VALOR REAL', 'VALOR PPTO']].sum()
        df_ecommerce_ing_neto = df_vyc_ecommerce[df_vyc_ecommerce['CONCEPTO'] == 'INGRESO NETO']
        df_ecommerce_ing_neto_porcj = df_ecommerce_ing_neto.copy()

        # Operar para obtener el porcentaje real y ppto
        df_ecommerce_ing_neto_porcj['PORCENTAJE REAL'] = (
            df_ecommerce_ing_neto_porcj['VALOR REAL'] / suma_ecommerce['VALOR REAL']
        )
        df_ecommerce_ing_neto_porcj['PORCENTAJE PPTO'] = (
            df_ecommerce_ing_neto_porcj['VALOR PPTO'] / suma_ecommerce['VALOR PPTO']
        )

        df_ecommerce_ing_neto_porcj = df_ecommerce_ing_neto_porcj.groupby('MARCA').agg({
            'PORCENTAJE REAL': 'sum', 
            'PORCENTAJE PPTO': 'sum',
            'CANAL': 'first', 
            'VALOR REAL': 'first',
            'VALOR PPTO': 'first'
        }).reset_index()

        # Operar el porcentaje por la venta en ecommerce
        df_ecommerce_ing_neto_porcj['VALOR REAL FINAL'] = (
            df_ecommerce_ing_neto_porcj['PORCENTAJE REAL']*valor_resta_operaciones_ecommerce_real
        )
        df_ecommerce_ing_neto_porcj['VALOR PPTO FINAL'] = (
            df_ecommerce_ing_neto_porcj['PORCENTAJE PPTO']*valor_resta_operaciones_ecommerce_ppto
        )

        df_g_operaciones_ecommerce = df_ecommerce_ing_neto_porcj.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL FINAL', 'VALOR PPTO FINAL']].sum().reset_index()
        df_totales_g_operaciones = df_totales_g_operaciones[df_totales_g_operaciones['CANAL'] != 'E-COMMERCE']
        df_totales_g_operaciones = df_totales_g_operaciones.drop(columns=['VALOR REAL', 'VALOR PPTO'])

        # DATAFRAME FINAL DE GASTOS DE OPERACIONES 'OK'
        df_bd_indirectos_g_operaciones = pd.concat(
            [df_totales_g_operaciones, df_g_operaciones_ecommerce], ignore_index=True
        )
        df_bd_indirectos_g_operaciones = df_bd_indirectos_g_operaciones.rename(
            columns={'VALOR REAL FINAL': 'VALOR REAL', 'VALOR PPTO FINAL': 'VALOR PPTO'}
        )
        df_bd_indirectos_g_operaciones['RUBRO'] = 'GASTOS DE OPERACIONES' 
        df_bd_indirectos_g_operaciones['DRIVER'] = 'GASTOS DE OPERACIONES'

        # ===================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'OTROS G. OPERACIONES' * #
        # ===================================================== #

        # Lista de tuplas con el nombre del centro de costo (ceco) y los porcentajes
        cecos = [
            ('BODEGA EXPORTACIONES', [0.12, 0.01, 0.58, 0.29]),
            ('MEJORAMIENTO FPT', [0.15, 0.15, 0.41, 0.29]),
        ]
        dfs = {}
        for ceco, porcentajes in cecos:
            df = filtrar_edg(ceco)
            dfs[ceco] = operar_porcj(df, porcentajes)

        # Operar FPT con los driver indirectos
        df_bd_indirectos_otros_g_operaciones = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])
        for key, df in dfs.items():
            df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_real['VALOR REAL'] = (
                df_merged_real['VALOR REAL CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            df_merged_real['VALOR PPTO'] = (
                df_merged_ppto['VALOR PPTO CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

            # Concatenar ambos cecos al dataframe principal 'OK'
            df_bd_indirectos_otros_g_operaciones = pd.concat(
                [df_bd_indirectos_otros_g_operaciones, 
                df_merged_real[columns_to_add]], 
                ignore_index=False
            )
        del df_merged_real, df_merged_ppto

        # FPT CHECK - Repartir Retail por canal
        df_valores_filtrados = df_vyc[
        (~df_vyc['CANAL'].isin(['FABRIC PARA TERCEROS', 'E-COMMERCE']))
        & (df_vyc['CONCEPTO'] == 'INGRESO NETO')]

        # Obtener los valores por marca canal
        df_total_marca = df_valores_filtrados.groupby('MARCA')[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_total_marca = df_total_marca.rename(columns={'VALOR REAL': 'SUMA REAL TOTAL', 'VALOR PPTO': 'SUMA PPTO TOTAL'})
        df_valores_marca_canal = df_valores_filtrados.groupby(['MARCA', 'CANAL'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_marca_canal = df_valores_marca_canal.rename(columns={'VALOR REAL': 'VALOR REAL MARCA', 'VALOR PPTO': 'VALOR PPTO MARCA'})

        # Obtener el porcentaje por marca y canal
        df_porcj_canal_marca = pd.merge(df_valores_marca_canal, df_total_marca, on='MARCA')
        df_porcj_canal_marca['PORCENTAJE REAL'] = df_porcj_canal_marca['VALOR REAL MARCA'] / df_porcj_canal_marca['SUMA REAL TOTAL']
        df_porcj_canal_marca['PORCENTAJE PPTO'] = df_porcj_canal_marca['VALOR PPTO MARCA'] / df_porcj_canal_marca['SUMA PPTO TOTAL']

        # Combinar los dataframe para operar 
        df_merged = pd.merge(
            df_bd_indirectos_otros_g_operaciones[df_bd_indirectos_otros_g_operaciones['CANAL'] != 'FABRIC PARA TERCEROS'],
            df_porcj_canal_marca, on='MARCA', how='left'
        )
        df_merged['VALOR REAL FINAL'] = df_merged['PORCENTAJE REAL']*df_merged['VALOR REAL']
        df_merged['VALOR PPTO FINAL'] = df_merged['PORCENTAJE PPTO']*df_merged['VALOR PPTO']

        df_merged = df_merged.drop(columns=['VALOR REAL', 'VALOR PPTO'])
        df_merged = df_merged.rename(columns={
            'CANAL_y': 'CANAL', 'VALOR REAL FINAL': 'VALOR REAL',
            'VALOR PPTO FINAL': 'VALOR PPTO'
        })
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

        # DATAFRAME FINAL DE OTROS G. OPERACIONES 'OK'
        df_bd_indirectos_otros_g_operaciones =  df_bd_indirectos_otros_g_operaciones[
            df_bd_indirectos_otros_g_operaciones['CANAL'] == 'FABRIC PARA TERCEROS'
        ]
        df_bd_indirectos_otros_g_operaciones = pd.concat(
        [df_bd_indirectos_otros_g_operaciones, 
        df_merged[columns_to_add]], 
        ignore_index=False
        )
        df_bd_indirectos_otros_g_operaciones['RUBRO'] = 'OTROS G. OPERACIONES'

        # =================== CODIGO VENTAS CANAL =================== # 'CHECK PPTO'

        # ======================================================= #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS DE VENTAS CANAL' * #
        # ======================================================= #

        # Cecos para el canal fpt
        cecos = [
            ('DEMAND OWNER', [0, 0, 0, 1]),
            ('EXPERTOS LOCALES', [0, 0, 1, 0]),
            ('ESTADOS UNIDOS', [0, 0, 0, 1]),
        ]

        dfs = {}
        for ceco, porcentajes in cecos:
            df = filtrar_edg(ceco)
            dfs[ceco] = operar_porcj(df, porcentajes)

        df_bd_indirectos_g_ventas_canal = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])

        # Combinar con los drivers y operar real y ppto
        for key, df in dfs.items():
            df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == key].merge(df, on='SEGMENTO')
            df_merged_real['VALOR REAL'] = (
                df_merged_real['VALOR REAL CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            df_merged_real['VALOR PPTO'] = (
                df_merged_ppto['VALOR PPTO CALCULADO'] * df_merged_real['VALOR PORCENTAJE']
            )
            columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

            # Concatenar el df combinado al df bd final
            df_bd_indirectos_g_ventas_canal = pd.concat(
                [df_bd_indirectos_g_ventas_canal, 
                df_merged_real[columns_to_add]], 
                ignore_index=False
            )
        del df_merged_real, df_merged_ppto


        # Repartir otros cecos para el resto de canales
        df_gvc = df_edg[
            ((df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') |
            (df_edg['CONCEPTO'] == 'GRAN MODERNO')) &
            (df_edg['CANAL'] != 'FABRIC PARA TERCEROS') &
            (df_edg['CANAL'] != '0')
        ]

        # Dataframe con el total real por marca canal
        df_valores_gvc = df_gvc.groupby(
            ['MARCA', 'CANAL', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_gvc = df_valores_gvc[
            (df_valores_gvc['MARCA'] == 'NO APLICA')
        ]

        # Esta parte de exportaciones cae derecho (se insluye al final)
        df_exportaciones = df_edg[
            ((df_edg['CECO'] == '11517030') | 
            (df_edg['CECO'] == '11517040') | 
            (df_edg['CECO'] == '11517010')) &
            (df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') |
            ((df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') &
            (df_edg['CANAL'] == 'EXPORTACIONES'))
        ]
        df_exportaciones = df_exportaciones[df_exportaciones['MARCA'] != 'NO APLICA']

        df_valor_exp = df_exportaciones.groupby(
            ['CANAL', 'MARCA', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Esta parte de Almacenes cae derecho (se insluye al final)
        df_tiendas_propias = df_edg[
            ((df_edg['CECO'] == '11541000') | 
            (df_edg['CECO'] == '11542000')) &
            (df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL')
        ]
        df_valor_tien_prop = df_tiendas_propias.groupby(
            ['CANAL', 'MARCA', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Esta parte de Professional cae derecho (se insluye al final)
        df_professional = df_edg[
            ((df_edg['CECO'] == 'T1519000') | 
            (df_edg['CECO'] == '11519003')) &
            (df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') &
            (df_edg['CANAL'] == 'PROFESSIONAL')
        ]
        df_valor_pro = df_professional.groupby(
            ['CANAL', 'MARCA', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()



        df_mod = df_edg[
            (df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') &
            (df_edg['CANAL'] == 'MODERNO') &
            (df_edg['CANAL'] != 'FABRIC PARA TERCEROS') &
            (df_edg['CANAL'] != '0')
        ]
        # Dataframe con el total real por marca canal
        df_valores_mod = df_mod.groupby(
            ['MARCA', 'CANAL', 'CECO', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Combinar los cecos con los driver
        df_merged_vc_moderno = pd.merge(
            df_drivers_ventas_canal, df_valores_mod,
            on='CECO', how='inner'
        )

        df_merged_vc_moderno['VALOR REAL'] = pd.to_numeric(df_merged_vc_moderno['VALOR REAL'], errors='coerce').fillna(0.0)/1000000
        df_merged_vc_moderno['VALOR PPTO'] = pd.to_numeric(df_merged_vc_moderno['VALOR PPTO'], errors='coerce').fillna(0.0)/1000000

        # Operar en el df combinado 
        df_merged_vc_moderno['VALOR REAL MP'] = (df_merged_vc_moderno['VALOR REAL']*df_merged_vc_moderno['MARCAS PROPIAS'])
        df_merged_vc_moderno['VALOR REAL ENL'] = (df_merged_vc_moderno['VALOR REAL']*df_merged_vc_moderno['EXP. NO LOCALES'])
        df_merged_vc_moderno['VALOR PPTO MP'] = (df_merged_vc_moderno['VALOR PPTO']*df_merged_vc_moderno['MARCAS PROPIAS'])
        df_merged_vc_moderno['VALOR PPTO ENL'] = (df_merged_vc_moderno['VALOR PPTO']*df_merged_vc_moderno['EXP. NO LOCALES'])

        columnas_mp = ['CANAL', 'MARCA', 'VALOR REAL MP', 'VALOR PPTO MP']
        columnas_enl = ['CANAL', 'MARCA', 'VALOR REAL ENL', 'VALOR PPTO ENL']

        df_merged_vc_moderno_mp = df_merged_vc_moderno[columnas_mp].copy()
        df_merged_vc_moderno_enl = df_merged_vc_moderno[columnas_enl].copy()
        df_merged_vc_moderno_mp['SEGMENTO'] = 'MARCAS PROPIAS'
        df_merged_vc_moderno_enl['SEGMENTO'] = 'EXP. NO LOCALES'

        df_merged_vc_moderno_mp = df_merged_vc_moderno_mp.rename(columns={
            'VALOR REAL MP': 'VALOR REAL', 'VALOR PPTO MP': 'VALOR PPTO'
        })
        df_merged_vc_moderno_enl = df_merged_vc_moderno_enl.rename(columns={
            'VALOR REAL ENL': 'VALOR REAL', 'VALOR PPTO ENL': 'VALOR PPTO'
        })

        df_merged_vc_moderno_mp_enl = pd.concat(
            [df_merged_vc_moderno_mp, df_merged_vc_moderno_enl],
            ignore_index=True
        )
        df_merged_vc_moderno_mp_enl = df_merged_vc_moderno_mp_enl.drop(columns='MARCA')

        df_merged_vc_moderno_mp_enl['DRIVER'] = 'GASTOS DE VENTAS CANAL'

        # Drivers para el canal moderno
        df_drivers_moderno_vc = df_drivers_moderno[(df_drivers_moderno['DRIVER'] == 'GASTOS DE VENTAS CANAL') & (df_drivers_moderno['CANAL'] == 'MODERNO')]

        # Combinar los drivers con el dataframe combinado
        df_merged_vc_mod = pd.merge(
            df_drivers_moderno_vc, df_merged_vc_moderno_mp_enl, 
            on=['CANAL', 'SEGMENTO']
        )

        df_merged_vc_mod['VALOR REAL'] = df_merged_vc_mod['VALOR REAL']* df_merged_vc_mod['VALOR PORCENTAJE']
        df_merged_vc_mod['VALOR PPTO'] = df_merged_vc_mod['VALOR PPTO']* df_merged_vc_mod['VALOR PORCENTAJE']

        columnas = ['MARCA', 'CANAL', 'SEGMENTO', 'VALOR REAL', 'VALOR PPTO']
        df_merged_vc_mod = df_merged_vc_mod[columnas]


        df_pac = df_edg[
            (df_edg['CONCEPTO'] == 'PAC') &
            (df_edg['CANAL'] != 'FABRIC PARA TERCEROS') &
            (df_edg['CANAL'] != '0')
        ]
        # Dataframe con el total real por marca canal
        df_valores_pac = df_pac.groupby(
            ['MARCA', 'CANAL', 'CECO', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Combinar los cecos con los driver
        df_merged = pd.merge(
            df_drivers_ventas_canal, df_valores_pac,
            on='CECO', how='inner'
        )

        df_merged['VALOR REAL'] = pd.to_numeric(df_merged['VALOR REAL'], errors='coerce').fillna(0.0)/1000000
        df_merged['VALOR PPTO'] = pd.to_numeric(df_merged['VALOR PPTO'], errors='coerce').fillna(0.0)/1000000

        # Operar en el df combinado 
        df_merged['VALOR REAL MP'] = (df_merged['VALOR REAL']*df_merged['MARCAS PROPIAS'])
        df_merged['VALOR REAL ENL'] = (df_merged['VALOR REAL']*df_merged['EXP. NO LOCALES'])
        df_merged['VALOR PPTO MP'] = (df_merged['VALOR PPTO']*df_merged['MARCAS PROPIAS'])
        df_merged['VALOR PPTO ENL'] = (df_merged['VALOR PPTO']*df_merged['EXP. NO LOCALES'])

        # Obtener el total de marcas propias 
        columnas_mp = ['CANAL', 'VALOR REAL MP', 'VALOR PPTO MP']
        df_total_mp = df_merged.copy()[columnas_mp].groupby('CANAL')[
            ['VALOR REAL MP', 'VALOR PPTO MP']
        ].sum().reset_index()
        df_total_mp['SEGMENTO'] = 'MARCAS PROPIAS'
        df_total_mp = df_total_mp.rename(columns={
            'VALOR REAL MP': 'VALOR REAL', 'VALOR PPTO MP': 'VALOR PPTO'
        })

        # Obtener el total de expertos no locales 
        columnas_enl = ['CANAL', 'VALOR REAL ENL', 'VALOR PPTO ENL']
        df_total_enl = df_merged.copy()[columnas_enl].groupby('CANAL')[
            ['VALOR REAL ENL', 'VALOR PPTO ENL']
        ].sum().reset_index()
        df_total_enl['SEGMENTO'] = 'EXP. NO LOCALES'
        df_total_enl = df_total_enl.rename(columns={
            'VALOR REAL ENL': 'VALOR REAL', 'VALOR PPTO ENL': 'VALOR PPTO'
        })

        # Usar el total de pac para restar las marcas
        from scripts.cmi.pac import hoja_pac
        df_pac_mapeo = hoja_pac(año, mes)

        # Insertar los segmentos a pac para separarlos
        df_pac_mapeo = utils.insert_segmentos(df_pac_mapeo)

        # Obtener el total de ambos segmentos
        df_pac_mp = df_pac_mapeo[df_pac_mapeo['SEGMENTO'] == 'MARCAS PROPIAS'][['VALOR REAL', 'VALOR PPTO']].sum()
        df_pac_enl = df_pac_mapeo[df_pac_mapeo['SEGMENTO'] == 'EXP. NO LOCALES'][['VALOR REAL', 'VALOR PPTO']].sum()

        # Restar el total de las marcas pac al total segmentos
        df_total_mp['VALOR REAL'] = df_total_mp['VALOR REAL'] - df_pac_mp['VALOR REAL']
        df_total_mp['VALOR PPTO'] = df_total_mp['VALOR PPTO'] - df_pac_mp['VALOR PPTO']
        df_total_enl['VALOR REAL'] = df_total_enl['VALOR REAL'] - df_pac_enl['VALOR REAL']
        df_total_enl['VALOR PPTO'] = df_total_enl['VALOR PPTO'] - df_pac_enl['VALOR PPTO']

        # Concatenar los df
        df_total_mp_enl = pd.concat([df_total_mp, df_total_enl], ignore_index=True)

        df_total_mp_enl['DRIVER'] = 'PAC'

        # Drivers para el canal moderno
        df_drivers_moderno_pac = df_drivers_moderno[(df_drivers_moderno['DRIVER'] == 'PAC') & (df_drivers_moderno['CANAL'] == 'MODERNO')]

        # Combinar los drivers con el dataframe combinado
        df_merged_pac_mod = pd.merge(
            df_drivers_moderno_pac, df_total_mp_enl, 
            on=['CANAL', 'SEGMENTO']
        )

        df_merged_pac_mod['VALOR REAL'] = df_merged_pac_mod['VALOR REAL']* df_merged_pac_mod['VALOR PORCENTAJE']
        df_merged_pac_mod['VALOR PPTO'] = df_merged_pac_mod['VALOR PPTO']* df_merged_pac_mod['VALOR PORCENTAJE']

        columnas = ['MARCA', 'CANAL', 'SEGMENTO', 'VALOR REAL', 'VALOR PPTO']
        df_merged_pac_mod = df_merged_pac_mod[columnas]

        # Dataframe con el total real y ppto por canal de Gran moderno
        df_gm_filtered = df_valores_gvc[
            (df_valores_gvc['CONCEPTO'] == 'GRAN MODERNO')
        ]
        df_gm_filtered = df_gm_filtered.groupby(
            ['CANAL', 'CONCEPTO'], as_index=False
        )[['VALOR REAL', 'VALOR PPTO']].sum()
        df_gm_filtered['VALOR REAL'] = df_gm_filtered['VALOR REAL']/1000000
        df_gm_filtered['VALOR PPTO'] = df_gm_filtered['VALOR PPTO']/1000000


        # Dataframe de porcentajes exclusivos para 3 Canales
        df_gm = df_valores_gvc[df_valores_gvc['CONCEPTO'] == 'GRAN MODERNO']
        porcj_meh = {
            'CANAL': ['MODERNO', 'EXPORTACIONES', 'HARD DISCOUNTERS'],
            'PORCENTAJE': [0.65, 0.1, 0.25]
        }
        df_porcj_gm = pd.DataFrame(porcj_meh)

        df_porcj_gm['VALOR REAL'] = df_porcj_gm['PORCENTAJE'] * df_gm['VALOR REAL'].iloc[0]
        df_porcj_gm['VALOR PPTO'] = df_porcj_gm['PORCENTAJE'] * df_gm['VALOR PPTO'].iloc[0]
        df_porcj_gm['CONCEPTO'] = 'GRAN MODERNO'
        df_porcj_gm = df_porcj_gm.drop(columns='PORCENTAJE')
        df_porcj_gm[['VALOR REAL', 'VALOR PPTO']] = df_porcj_gm[['VALOR REAL', 'VALOR PPTO']]/1000000

        # Repartir entre segmentos gran moderno
        porcj_segmentos_mod = {
            'CANAL': 'MODERNO',
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS'],
            'PORCENTAJE': [0.622, 0.378]
        }
        df_porcj_seg_mod = pd.DataFrame(porcj_segmentos_mod)

        porcj_segmentos_hd = {
            'CANAL': 'HARD DISCOUNTERS',
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS'],
            'PORCENTAJE': [0, 1]
        }
        df_porcj_seg_hd= pd.DataFrame(porcj_segmentos_hd)

        porcj_segmentos_exp = {
            'CANAL': 'EXPORTACIONES',
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS'],
            'PORCENTAJE': [0.25, 0.75]
        }
        df_porcj_seg_exp = pd.DataFrame(porcj_segmentos_exp)

        # Combinar en un solo DataFrame
        df_combined = pd.concat(
            [df_porcj_seg_mod, df_porcj_seg_hd, df_porcj_seg_exp], 
            ignore_index=True
        )

        # Combinar los valores de gran moderno con los porcentajes
        df_merged_gm = pd.merge(
            df_porcj_gm, df_combined,
            on='CANAL', how='left'
        )

        # Operar los porcentajes con el valor real y ppto
        df_merged_gm['VALOR REAL'] = df_merged_gm['VALOR REAL']*df_merged_gm['PORCENTAJE']
        df_merged_gm['VALOR PPTO'] = df_merged_gm['VALOR PPTO']*df_merged_gm['PORCENTAJE']


        df_merged_gm = df_merged_gm.drop(columns='PORCENTAJE')

        # Drivers para el canal moderno
        df_drivers_moderno_gm = df_drivers_moderno[
            (df_drivers_moderno['DRIVER'] == 'GRAN MODERNO') & 
            (df_drivers_moderno['CANAL'] == 'MODERNO')
        ]
        df_drivers_hd_gm = df_drivers_moderno[
            (df_drivers_moderno['DRIVER'] == 'GRAN MODERNO') & 
            (df_drivers_moderno['CANAL'] == 'HARD DISCOUNTERS')
        ]
        df_drivers_exp_gm = df_drivers_moderno[
            (df_drivers_moderno['DRIVER'] == 'GRAN MODERNO') & 
            (df_drivers_moderno['CANAL'] == 'EXPORTACIONES')
        ]

        # Combinar los drivers con el dataframe combinado
        df_merged_gm_mod = pd.merge(
            df_drivers_moderno_gm, df_merged_gm, 
            on=['CANAL', 'SEGMENTO']
        )

        # Combinar los drivers con el dataframe combinado
        df_merged_gm_hd = pd.merge(
            df_drivers_hd_gm, df_merged_gm, 
            on=['CANAL', 'SEGMENTO']
        )

        # Combinar los drivers con el dataframe combinado
        df_merged_gm_exp = pd.merge(
            df_drivers_exp_gm, df_merged_gm, 
            on=['CANAL', 'SEGMENTO']
        )

        df_merged_gm_mod['VALOR REAL'] = df_merged_gm_mod['VALOR REAL']* df_merged_gm_mod['VALOR PORCENTAJE']
        df_merged_gm_mod['VALOR PPTO'] = df_merged_gm_mod['VALOR PPTO']* df_merged_gm_mod['VALOR PORCENTAJE']

        df_merged_gm_hd['VALOR REAL'] = df_merged_gm_hd['VALOR REAL']* df_merged_gm_hd['VALOR PORCENTAJE']
        df_merged_gm_hd['VALOR PPTO'] = df_merged_gm_hd['VALOR PPTO']* df_merged_gm_hd['VALOR PORCENTAJE']

        df_merged_gm_exp['VALOR REAL'] = df_merged_gm_exp['VALOR REAL']* df_merged_gm_exp['VALOR PORCENTAJE']
        df_merged_gm_exp['VALOR PPTO'] = df_merged_gm_exp['VALOR PPTO']* df_merged_gm_exp['VALOR PORCENTAJE']


        columnas = ['MARCA', 'CANAL', 'SEGMENTO', 'VALOR REAL', 'VALOR PPTO']
        df_merged_gm_mod = df_merged_gm_mod[columnas]
        df_merged_gm_hd = df_merged_gm_hd[columnas]
        df_merged_gm_exp = df_merged_gm_exp[columnas]

        # TODO - ATENCION AQUI CONCATENAR AL FINAL

        df_bd_indirectos_g_ventas_canal_moderno = pd.concat(
            [df_merged_vc_mod, df_merged_pac_mod, df_merged_gm_mod, df_merged_gm_exp, df_merged_gm_hd]
        )
        df_bd_indirectos_g_ventas_canal_moderno = df_bd_indirectos_g_ventas_canal_moderno.drop(
            columns=['SEGMENTO']
        )
        df_bd_indirectos_g_ventas_canal_moderno[['RUBRO', 'DRIVER']] = 'GASTOS DE VENTAS CANAL'

        # Dataframe con el total real y ppto por canal de Admon tiendas de belleza
        df_atb_filtered = df_edg[
            (df_edg['CONCEPTO'] == 'ADMON TIENDAS DE BELLEZA')
        ]
        df_valores_atb = df_atb_filtered.groupby(
            ['CANAL', 'MARCA', 'CONCEPTO']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Dataframe de porcentajes exclusivos ADMON TIENDAS DE BELLEZA
        porcj_atb = {
            'CANAL': ['TIENDAS DE BELLEZA', 'PROFESSIONAL'],
            'PORCENTAJE': [0.5, 0.5]}
        df_porcj_atb = pd.DataFrame(porcj_atb)

        df_porcj_atb['VALOR REAL'] = df_porcj_atb['PORCENTAJE'] * df_valores_atb['VALOR REAL'].iloc[0]
        df_porcj_atb['VALOR PPTO'] = df_porcj_atb['PORCENTAJE'] * df_valores_atb['VALOR PPTO'].iloc[0]
        df_porcj_atb['CONCEPTO'] = 'ADMON TIENDAS DE BELLEZA'
        df_porcj_atb = df_porcj_atb.drop(columns='PORCENTAJE')

        # Combinar los df con sus respectivos porcentajes
        df_atb_filtered = df_porcj_atb
        df_atb_filtered['VALOR REAL'] = df_atb_filtered['VALOR REAL']/1000000
        df_atb_filtered['VALOR PPTO'] = df_atb_filtered['VALOR PPTO']/1000000

        # Operar el concepto Admon tiendas de belleza real
        df_drivers_real_atb = df_drivers_real[
            (df_drivers_real['DRIVER'] == 'ADMON TIENDAS DE BELLEZA')
        ]
        df_merged_real_atb = pd.merge(
            df_atb_filtered, 
            df_drivers_real_atb,
            on='CANAL', how='left'
        )

        # Operar el concepto Admon tiendas de belleza ppto
        df_drivers_ppto_atb = df_drivers_ppto[
            (df_drivers_ppto['DRIVER'] == 'ADMON TIENDAS DE BELLEZA')
        ]
        df_merged_ppto_atb = pd.merge(
            df_atb_filtered, 
            df_drivers_ppto_atb,
            on='CANAL', how='left'
        )

        # Hacer las operaciones de Admon tiendas de belleza
        df_merged_real_atb['VALOR REAL'] = (
            df_merged_real_atb['VALOR REAL'] * df_merged_real_atb['VALOR PORCENTAJE']
        )
        df_merged_ppto_atb['VALOR PPTO'] = (
            df_merged_ppto_atb['VALOR PPTO'] * df_merged_ppto_atb['VALOR PORCENTAJE']
        )

        # Dataframe con el total real y ppto por canal de gvc
        df_gvc_filtered = df_valores_gvc[
            (df_valores_gvc['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') &
            (df_valores_gvc['CANAL'] != 'ALMACENES INTERNOS') & 
            (df_valores_gvc['CANAL'] != 'MODERNO') &
            (df_valores_gvc['CANAL'] != 'E-COMMERCE') &
            (df_valores_gvc['CANAL'] != 'TIENDAS PROPIAS')
        ]
        df_gvc_filtered = df_gvc_filtered.groupby(
            ['CANAL','CONCEPTO'], as_index=False
        )[['VALOR REAL', 'VALOR PPTO']].sum()
        df_gvc_filtered['VALOR REAL'] = df_gvc_filtered['VALOR REAL']/1000000
        df_gvc_filtered['VALOR PPTO'] = df_gvc_filtered['VALOR PPTO']/1000000


        # Operar el concepto gastos de ventas canal real
        df_drivers_real_gvc = df_drivers_real[
            (df_drivers_real['DRIVER'] == 'GASTOS DE VENTAS CANAL')
        ]
        df_merged_real_gvc = pd.merge(
            df_gvc_filtered, 
            df_drivers_real_gvc,
            on='CANAL', how='left'
        )

        # Operar el concepto gastos de ventas canal ppto
        df_drivers_ppto_gvc = df_drivers_ppto[
            (df_drivers_ppto['DRIVER'] == 'GASTOS DE VENTAS CANAL')
        ]
        df_merged_ppto_gvc = pd.merge(
            df_gvc_filtered, 
            df_drivers_ppto_gvc,
            on='CANAL', how='left'
        )

        # Hacer las operaciones de gastos de ventas canal
        df_merged_real_gvc['VALOR REAL'] = (
            df_merged_real_gvc['VALOR REAL'] * df_merged_real_gvc['VALOR PORCENTAJE']
        )
        df_merged_ppto_gvc['VALOR PPTO'] = (
            df_merged_ppto_gvc['VALOR PPTO'] * df_merged_ppto_gvc['VALOR PORCENTAJE']
        )

        # Columnas a añadir del dataframe combinado
        columns_to_add_real = ['CANAL', 'MARCA', 'DRIVER', 'RUBRO', 'VALOR REAL']
        columns_to_add_ppto = ['CANAL', 'MARCA', 'DRIVER', 'RUBRO', 'VALOR PPTO']

        # Dataframe filtrado de gastos de ventas canal
        df_merged_real_gvc = df_merged_real_gvc[columns_to_add_real]
        df_merged_ppto_gvc = df_merged_ppto_gvc[columns_to_add_ppto]

        # Dataframe filtrado de admon tiendas de belleza
        df_merged_real_atb = df_merged_real_atb[columns_to_add_real]
        df_merged_ppto_atb = df_merged_ppto_atb[columns_to_add_ppto]

        # Concatenar todos los dataframe combinados en uno solo
        df_merged_final_real = pd.concat(
            [df_merged_real_gvc, df_merged_real_atb], 
            ignore_index=True

        )
        df_merged_final_ppto = pd.concat(
            [df_merged_ppto_gvc, df_merged_ppto_atb], 
            ignore_index=True
        )

        # Combinar el real y el ppto
        df_merged_final_real['VALOR PPTO'] = df_merged_final_ppto['VALOR PPTO']

        # Concatenar el df combinado al df bd final
        df_bd_indirectos_g_ventas_canal = pd.concat(
            [df_bd_indirectos_g_ventas_canal, 
            df_merged_final_real], 
            ignore_index=False
        )
        df_bd_indirectos_g_ventas_canal['RUBRO'] = 'GASTOS DE VENTAS CANAL'

        df_bd_indirectos_g_ventas_canal = pd.concat(
            [df_bd_indirectos_g_ventas_canal, df_bd_indirectos_g_ventas_canal_moderno],
            ignore_index=False
        )

        # Eliminar filas con valor 0.0
        df_bd_indirectos_g_ventas_canal = df_bd_indirectos_g_ventas_canal[
            (~((df_bd_indirectos_g_ventas_canal['VALOR REAL'] == 0.0) & 
            (df_bd_indirectos_g_ventas_canal['VALOR PPTO'] == 0.0)))
        ]

        # Parte de E-Commerce para Ventas Canal (Filtrar ejecucion de gastos a 'ecommerce')
        df_ecommerce_vc = df_edg[
            (df_edg['CONCEPTO'] == 'GASTOS DE VENTAS CANAL') &
            ((df_edg['CANAL'] == 'E-COMMERCE') |
            (df_edg['CANAL'] == 'ALMACENES INTERNOS'))
        ]

        # Concatenar el valor de tiendas propias 
        df_ecommerce_vc = pd.concat([df_ecommerce_vc, df_valor_tien_prop], ignore_index=True)

        # Obtener los valores principales para ventas canal
        valor_ecommerce = df_ecommerce_vc[
            df_ecommerce_vc['CANAL'] == 'E-COMMERCE'
        ][['VALOR REAL', 'VALOR PPTO']].sum()
        valor_almacenes = df_ecommerce_vc[
            df_ecommerce_vc['CANAL'] == 'ALMACENES INTERNOS'
        ][['VALOR REAL', 'VALOR PPTO']].sum()

        # Repartir por los canales ecommerce Y almacenes
        df_vyc_ecommerce = df_vyc[
            (df_vyc['CANAL'] == 'E-COMMERCE') & 
            (df_vyc['CONCEPTO'] == 'VENTAS BRUTAS')
        ]
        suma_ecommerce = df_vyc_ecommerce[['VALOR REAL', 'VALOR PPTO']].sum()

        # Operar y obtener el porcentaje
        df_ecommerce_porcj = df_vyc_ecommerce.copy()
        df_ecommerce_porcj['PORCENTAJE REAL'] = (
            df_ecommerce_porcj['VALOR REAL'] / suma_ecommerce['VALOR REAL']
        )
        df_ecommerce_porcj['PORCENTAJE PPTO'] = (
            df_ecommerce_porcj['VALOR PPTO'] / suma_ecommerce['VALOR PPTO']
        )


        df_vyc_almacenes = df_vyc[
            (df_vyc['CANAL'] == 'ALMACENES') & 
            (df_vyc['CONCEPTO'] == 'VENTAS BRUTAS')
        ]
        suma_almacenes = df_vyc_almacenes[['VALOR REAL', 'VALOR PPTO']].sum()

        df_almacenes_porcj = df_vyc_almacenes.copy()
        df_almacenes_porcj['PORCENTAJE REAL'] = (
            df_almacenes_porcj['VALOR REAL'] / suma_almacenes['VALOR REAL']
        )
        df_almacenes_porcj['PORCENTAJE PPTO'] = (
            df_almacenes_porcj['VALOR PPTO'] / suma_almacenes['VALOR PPTO']
        )


        df_ecommerce_porcj = df_ecommerce_porcj.groupby('MARCA').agg({
            'PORCENTAJE REAL': 'sum', 
            'PORCENTAJE PPTO': 'sum', 
            'CANAL': 'first', 
            'VALOR REAL': 'first',
            'VALOR PPTO': 'first'
        }).reset_index()


        df_almacenes_porcj = df_almacenes_porcj.groupby('MARCA').agg({
            'PORCENTAJE REAL': 'sum', 
            'PORCENTAJE PPTO': 'sum', 
            'CANAL': 'first', 
            'VALOR REAL': 'first',
            'VALOR PPTO': 'first'
        }).reset_index()


        # Obtener el valor de e-commerce y operar por el procentaje
        df_ecommerce_porcj['VALOR REAL'] = (
            df_ecommerce_porcj['PORCENTAJE REAL']*valor_ecommerce['VALOR REAL']
        )/1000000
        df_ecommerce_porcj['VALOR PPTO'] = (
            df_ecommerce_porcj['PORCENTAJE PPTO']*valor_ecommerce['VALOR PPTO']
        )/1000000



        df_ecommerce_porcj = df_ecommerce_porcj.drop(columns=['PORCENTAJE REAL', 'PORCENTAJE PPTO'])
        df_ecommerce_porcj[['DRIVER', 'RUBRO']]  = 'GASTOS DE VENTAS CANAL'


        df_almacenes_porcj['VALOR REAL'] = (
            df_almacenes_porcj['PORCENTAJE REAL']*valor_almacenes['VALOR REAL']
        )/1000000
        df_almacenes_porcj['VALOR PPTO'] = (
            df_almacenes_porcj['PORCENTAJE PPTO']*valor_almacenes['VALOR PPTO']
        )/1000000

        df_almacenes_porcj = df_almacenes_porcj.drop(columns=['PORCENTAJE REAL', 'PORCENTAJE PPTO'])
        df_almacenes_porcj[['DRIVER', 'RUBRO']]  = 'GASTOS DE VENTAS CANAL'

        # Concatenar ambos canales
        df_almacenes_ecommerce = pd.concat(
            [df_almacenes_porcj, df_ecommerce_porcj], ignore_index=True
        )

        # Concatenar el df combinado al df bd final
        df_bd_indirectos_g_ventas_canal = pd.concat(
            [df_bd_indirectos_g_ventas_canal, 
            df_almacenes_ecommerce], 
            ignore_index=True
        )

        # Añadir de el canal exportaciones los valores faltantes
        df_valor_exp = df_valor_exp.copy()
        df_valor_exp['RUBRO'] = 'GASTOS DE VENTAS CANAL'
        df_valor_exp = df_valor_exp.rename(columns={'CONCEPTO': 'DRIVER'})
        df_valor_exp[['VALOR REAL', 'VALOR PPTO']] = (df_valor_exp[['VALOR REAL', 'VALOR PPTO']])/1000000

        # Concatenar el df combinado al df bd final
        df_bd_indirectos_g_ventas_canal = pd.concat(
            [df_bd_indirectos_g_ventas_canal, 
            df_valor_exp], 
            ignore_index=True
        )

        # Añadir de el canal professional los valores faltantes
        df_valor_pro = df_valor_pro.copy()
        df_valor_pro['RUBRO'] = 'GASTOS DE VENTAS CANAL'
        df_valor_pro = df_valor_pro.rename(columns={'CONCEPTO': 'DRIVER'})
        df_valor_pro[['VALOR REAL', 'VALOR PPTO']] = (df_valor_pro[['VALOR REAL', 'VALOR PPTO']])/1000000

        # Concatenar el df combinado al df bd final
        df_bd_indirectos_g_ventas_canal = pd.concat(
            [df_bd_indirectos_g_ventas_canal, 
            df_valor_pro], 
            ignore_index=True
        )

        # ============================================== #
        # * CONSOLIDAR TODOS LOS RUBROS EN UNA SOLA BD * #
        # ============================================== #

        dataframes_bd_indirectos = [
           df_bd_indirectos_g_compras_logistica,
           df_bd_indirectos_g_invest_dllo,
           df_bd_indirectos_g_operaciones,
           df_bd_indirectos_otros_g_operaciones,
           df_bd_indirectos_g_ventas_canal
        ]

        # Dataframe final de la base de datos directos
        df_bd_indirectos = pd.DataFrame(columns=[
            'MARCA', 'CANAL', 'DRIVER', 'VALOR REAL'
        ])
        df_bd_indirectos = pd.concat(dataframes_bd_indirectos, ignore_index=True)

        # Cambiar marcas
        df_bd_indirectos['MARCA'] = df_bd_indirectos['MARCA'].replace('AMAZON', 'OTROS CLIENTES DO')
        df_bd_indirectos['MARCA'] = df_bd_indirectos['MARCA'].replace('FAMILIA', 'OTROS EXP. LOCALES')

        # Insertar fecha
        df_bd_indirectos.insert(0, 'AÑO', año)
        df_bd_indirectos.insert(1, 'MES', mes_letras)

        # Insertar segmentos
        df_bd_indirectos = utils.insert_segmentos(df_bd_indirectos)

        # Organizar columnas
        df_bd_indirectos = df_bd_indirectos[
            ['AÑO', 'MES', 'MARCA', 'SEGMENTO', 'CANAL', 'DRIVER', 'RUBRO', 'VALOR REAL', 'VALOR PPTO']
        ]

        # Exportar base de datos mensual
        sub_dirs_final_db = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
            'GASTOS'
        ]
        path_bd_indirectos = os.path.join(
            base_dir, *sub_dirs_final_db, f'BD_INDIRECTOS_{mes}.{año}.xlsx'
        )
        df_bd_indirectos.to_excel(path_bd_indirectos, index=False)

        df_bd_indirectos = df_bd_indirectos.drop(columns='DRIVER')
        df_bd_indirectos = df_bd_indirectos.rename(columns={'RUBRO': 'DRIVER'})
        
        return df_bd_indirectos
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
        return False