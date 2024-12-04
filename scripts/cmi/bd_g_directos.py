from scripts.utils import utils
from controller.cmiController import vycController

def get_bd_gastos_directos(df_edg, mes, año, mes_largo):

    try:
        import pandas as pd
        import os

        # Ruta a directorio de cada bd
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

        # Ruta a cada base de datos externa
        vyc = os.path.join(base_dir, *sub_dirs_final_vyc, f'bd_venta_y_costo_{mes}.{año}.xlsx')
        drivers = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_directos.xlsx')
        drivers_merch_admon = os.path.join(base_dir, *sub_dirs_drivers, 'drivers_merch_admon.xlsx')

        # Leer las bases de datos externas
        df_vyc = pd.read_excel(vyc, engine='openpyxl', sheet_name='Sheet1')
        df_drivers = pd.read_excel(drivers)
        df_drivers_merch_admon = pd.read_excel(drivers_merch_admon)

        # Obtener mes y año
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

        # Segmentos
        segmentos = [
            'EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'
        ]

        # Marcas generales
        marcas_general = [
            'ACCESORIOS', 'BIO OIL', 'BURTS BEES', 'CADIVEU', 'CATRICE', 
            'ESSENCE', 'HASK', 'MAX FACTOR', 'MORROCCANOIL', 'NOPIKEX', 
            'COVER GIRL', 'OGX', 'OTROS EXP. NO LOCALES', 'WELLA CONSUMO',
            'WELLA PROFESSIONAL', 'ARDEN FOR MEN', 'ELIZABETH ARDEN', 'NUDE', 'VITÚ', 'YARDLEY', 
            'OTRAS MP', 'AVON', 'KIMBERLY', 'UNILEVER', 'NATURA', 'OMNILIFE', 
            'OTROS EXP. LOCALES', 'JERONIMO MARTINS', 'HENKEL', 'EL ÉXITO', 
            'OTROS CLIENTES DO', 'NOVAVENTA', 'LOCATEL', 'BEIERSDORF', 'D1', 
            'ALICORP', 'FISA', 'MILAGROS ENTERPRISE', 'WORMSER', 'SIN ASIGNAR'
        ]

        # Canales generales
        canales_general = [
            'MODERNO', 'TRADICIONAL', 'E-COMMERCE', 'ALMACENES', 'ALTERNATIVO', 
            'EXPORTACIONES', 'INSTITUCIONAL', 'PROFESSIONAL', 'TIENDAS DE BELLEZA',
            'FABRIC PARA TERCEROS', 'HARD DISCOUNTERS'
        ]

        # Canales marcas propias
        canales_mp = [
            'MODERNO', 'TRADICIONAL', 'E-COMMERCE', 'ALMACENES', 'ALTERNATIVO', 
            'EXPORTACIONES', 'HARD DISCOUNTERS', 'INSTITUCIONAL', 
            'PROFESSIONAL', 'TIENDAS DE BELLEZA'
        ]

        # Marcas marcas propias
        marcas_mp = [
            'ARDEN FOR MEN', 'ELIZABETH ARDEN', 'NUDE', 'VITÚ', 'YARDLEY', 
            'OTRAS MP'
        ]

        # Marcas expertos no locales
        marcas_enl = [
            'ACCESORIOS', 'BIO OIL', 'BURTS BEES', 'CADIVEU', 'CATRICE', 
            'ESSENCE', 'HASK', 'MAX FACTOR', 'MORROCCANOIL', 'NOPIKEX', 
            'COVER GIRL', 'OGX', 'OTROS EXP. NO LOCALES', 'WELLA CONSUMO',
            'WELLA PROFESSIONAL'
        ]

        # Canales expertos no locales
        canales_enl = [
            'MODERNO', 'TRADICIONAL', 'E-COMMERCE', 'ALMACENES', 'ALTERNATIVO', 
            'EXPORTACIONES', 'INSTITUCIONAL', 'PROFESSIONAL', 'TIENDAS DE BELLEZA'
        ]

        # Crear dataframe con filtros necesarios
        df_vyc = df_vyc[['CANAL', 'MARCA', 'SEGMENTO', 'CONCEPTO', 'VALOR REAL', 'VALOR PPTO']]
        df_drivers_real = df_drivers[df_drivers['AÑO'] == int(año)]
        df_drivers_ppto = df_drivers[df_drivers['AÑO'] == f"{año} P"]

        # Dataframe filtrados de venta y costo
        df_vyc_mp = df_vyc[(df_vyc['SEGMENTO'] == 'MARCAS PROPIAS')]
        df_vyc_enl = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. NO LOCALES')]
        df_vyc_el = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. LOCALES')]
        df_vyc_do = df_vyc[(df_vyc['SEGMENTO'] == 'DEMAND OWNERS')]

        # =============== CODIGO INVERSIÓN EN MERCADEO =============== # 'CHECK PPTO'

        # ====================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'INVERSION EN MERCADEO' * #
        # ====================================================== #

        df_inv_merc = df_edg[df_edg['CONCEPTO'] == 'INVERSIÓN EN MERCADEO']

        # Dataframe con el total real y ppto por canal
        df_valores_inv_merc = df_inv_merc.groupby('MARCA')[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_inv_merc.columns = ['MARCA', 'VALOR REAL', 'VALOR PPTO']

        # Dataframes combinado para operar con los drivers
        df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == 'INVERSIÓN EN MERCADEO'].merge(
            df_valores_inv_merc, on='MARCA'
        )
        df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == 'INVERSIÓN EN MERCADEO'].merge(
            df_valores_inv_merc, on='MARCA'
        )

        # Operar los dataframes combinados
        df_merged_real['VALOR REAL'] = (
            df_merged_real['VALOR REAL'] * df_merged_real['VALOR PORCENTAJE']
        )/1000000
        df_merged_real['VALOR PPTO'] = (
            df_merged_ppto['VALOR PPTO'] * df_merged_ppto['VALOR PORCENTAJE']
        )/1000000

        # Concatenar el df combinado al df bd final
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        df_bd_directos_inv_merc = pd.concat(
            [df_merged_real[columns_to_add]], ignore_index=True
        )
        del df_merged_real, df_merged_ppto

        # Repartir por el canal ecommerce
        df_vyc_ecommerce = df_vyc[
            (df_vyc['CANAL'] == 'E-COMMERCE') & 
            (df_vyc['CONCEPTO'] == 'INGRESO NETO')
        ]
        suma_ecommerce = df_vyc_ecommerce[['VALOR REAL', 'VALOR PPTO']].sum()

        # Opera y obtener el porcentaje
        df_ecommerce_porcj = df_vyc_ecommerce.copy()
        df_ecommerce_porcj['PORCENTAJE REAL'] = (
            df_ecommerce_porcj['VALOR REAL'] / suma_ecommerce['VALOR REAL']
        )
        df_ecommerce_porcj['PORCENTAJE PPTO'] = (
            df_ecommerce_porcj['VALOR PPTO'] / suma_ecommerce['VALOR PPTO']
        )

        df_ecommerce_porcj = df_ecommerce_porcj.groupby('MARCA').agg({
            'PORCENTAJE REAL': 'sum', 
            'PORCENTAJE PPTO': 'sum', 
            'CANAL': 'first', 
            'VALOR REAL': 'first',
            'VALOR PPTO': 'first'
        }).reset_index()

        # Obtener el valor de e-commerce y operar por el procentaje
        valor_ecommerce_inv_merc = df_valores_inv_merc[
            df_valores_inv_merc['MARCA'] == 'NO APLICA'
        ][['VALOR REAL', 'VALOR PPTO']].values[0]/1000000
        df_ecommerce_porcj['VALOR REAL'] = df_ecommerce_porcj['PORCENTAJE REAL']*valor_ecommerce_inv_merc[0]
        df_ecommerce_porcj['VALOR PPTO'] = df_ecommerce_porcj['PORCENTAJE PPTO']*valor_ecommerce_inv_merc[1]

        df_ecommerce_porcj = df_ecommerce_porcj.drop(columns=['PORCENTAJE REAL', 'PORCENTAJE PPTO'])
        df_ecommerce_porcj['DRIVER']  = 'INVERSIÓN EN MERCADEO'

        df_bd_directos_inv_merc = pd.concat(
            [df_bd_directos_inv_merc, df_ecommerce_porcj], ignore_index=True
        )
        df_bd_directos_inv_merc[['VALOR REAL', 'VALOR PPTO']] = df_bd_directos_inv_merc[['VALOR REAL', 'VALOR PPTO']].fillna(0)
        df_bd_directos_inv_merc = df_bd_directos_inv_merc[
            (~((df_bd_directos_inv_merc['VALOR REAL'] == 0.0) & (df_bd_directos_inv_merc['VALOR PPTO'] == 0.0))) 
        ]

        # ========================================== #
        # * REPARTIR CANALES VACIOS DE LA BD FINAL * #
        # ========================================== #

        # Filtrar dataframes necesarios
        df_bd_directos_inv_merc = df_bd_directos_inv_merc.fillna('')
        df_gd = df_bd_directos_inv_merc[
            (df_bd_directos_inv_merc['CANAL'] == '')
        ]
        df_valores_filtrados = df_vyc[
            (~df_vyc['CANAL'].isin(['FABRIC PARA TERCEROS', 'E-COMMERCE'])) & 
            (df_vyc['CONCEPTO'] == 'INGRESO NETO')
        ]

        # Obtener los valores por marca canal
        df_total_marca = df_valores_filtrados.groupby('MARCA')[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_total_marca = df_total_marca.rename(columns={'VALOR REAL': 'SUMA REAL TOTAL', 'VALOR PPTO': 'SUMA PPTO TOTAL'})
        df_valores_marca_canal = df_valores_filtrados.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_marca_canal = df_valores_marca_canal.rename(columns={'VALOR REAL': 'VALOR REAL MARCA', 'VALOR PPTO': 'VALOR PPTO MARCA'})

        # Obtener el porcentaje de marca canal
        df_porcj_marca_canal = pd.merge(df_valores_marca_canal, df_total_marca, on='MARCA')
        df_porcj_marca_canal['PORCENTAJE REAL'] = (
            df_porcj_marca_canal['VALOR REAL MARCA'] / df_porcj_marca_canal['SUMA REAL TOTAL']
        )
        df_porcj_marca_canal['PORCENTAJE PPTO'] = (
            df_porcj_marca_canal['VALOR PPTO MARCA'] / df_porcj_marca_canal['SUMA PPTO TOTAL']
        )

        # Combinar los dataframe para operar
        df_gd_merged = pd.merge(
            df_gd, df_porcj_marca_canal, on='MARCA', how='left'
        )
        df_gd_merged['VALOR REAL'] = pd.to_numeric(df_gd_merged['VALOR REAL'], errors='coerce')
        df_gd_merged['VALOR PPTO'] = pd.to_numeric(df_gd_merged['VALOR PPTO'], errors='coerce')

        df_gd_merged['VALOR REAL'] = (
            df_gd_merged['VALOR REAL'] * df_gd_merged['PORCENTAJE REAL']
        ) 
        df_gd_merged['VALOR PPTO'] = (
            df_gd_merged['VALOR PPTO'] * df_gd_merged['PORCENTAJE PPTO']
        ) 

        # Eliminar columnas innecesarias
        df_gd_merged = df_gd_merged.drop(columns=[
            'CANAL_x', 'VALOR REAL MARCA', 'VALOR PPTO MARCA', 'SUMA REAL TOTAL',
            'SUMA PPTO TOTAL', 'PORCENTAJE REAL', 'PORCENTAJE PPTO'
        ])

        # Renombrar columnas
        df_gd_merged = df_gd_merged.rename(columns={'CANAL_y': 'CANAL'})

        # Concatenar al df final
        df_bd_directos_inv_merc = pd.concat([df_bd_directos_inv_merc, df_gd_merged], ignore_index=True)

        # Eliminar los canales vacios
        df_bd_directos_inv_merc = df_bd_directos_inv_merc.fillna('')
        df_bd_directos_inv_merc = df_bd_directos_inv_merc[df_bd_directos_inv_merc['CANAL'] != '']

        # Eliminar filas donde el real y el ppto sean cero absoluto
        df_bd_directos_inv_merc = df_bd_directos_inv_merc[
            (~((df_bd_directos_inv_merc['VALOR REAL'] == 0.0) & (df_bd_directos_inv_merc['VALOR PPTO'] == 0.0)))
        ]
        
        # ===================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'ADMINISTRACION MARCA' * #
        # ===================================================== #

        # BAJAR LA INFORMACION DEL CUBO, TRANSFORMARLA Y GUARDARLA
        controlador = vycController.vycController()
        data = controlador.obtener_datos_sap(año, mes_largo)
        df_bd_directos_admin_marca = controlador.transformar_datos_gasto_mercadeo(data)
        df_bd_directos_admin_marca = df_bd_directos_admin_marca.rename(
            columns={'CONCEPTO': 'DRIVER'}
            )
        
        # DATAFRAME COMBINADO PARA OPERAR CON LOS DRIVERS
        df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == 'ADMINISTRACIÓN MARCA'].merge(
            df_bd_directos_admin_marca, on='MARCA'
        )
        df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == 'ADMINISTRACIÓN MARCA'].merge(
            df_bd_directos_admin_marca, on='MARCA'
        )

        df_merged_real = df_merged_real.rename(columns={'VALOR REAL': 'VALOR NO REPARTIDO'})
        df_merged_ppto = df_merged_ppto.rename(columns={'VALOR PPTO': 'VALOR NO REPARTIDO'})

        df_merged_real['VALOR REAL'] = (df_merged_real['VALOR NO REPARTIDO'] * df_merged_real['VALOR PORCENTAJE'])
        df_merged_real['VALOR PPTO'] = (df_merged_ppto['VALOR NO REPARTIDO'] * df_merged_ppto['VALOR PORCENTAJE'])

        df_merged_real = df_merged_real.rename(columns={'CANAL_x': 'CANAL', 'DRIVER_x': 'DRIVER'})
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
       
        # DATAFRAME FINAL DE ADMINISTRACION MARCA 'OK'
        df_bd_directos_admin_marca = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])
        df_bd_directos_admin_marca = pd.concat(
            [df_bd_directos_admin_marca, df_merged_real[columns_to_add]], ignore_index=True
        )
        
        del df_merged_real, df_merged_ppto

        # ==================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'MERCHANDISING ADMON' * #
        # ==================================================== #

        # Dataframe filtrado para merchandising admon
        df_merch_admon = df_edg[
            (df_edg['CONCEPTO'] == 'INVERSIÓN EN MERCHANDISING') & 
            (df_edg['MARCA'] == 'NO APLICA') & 
            (df_edg['CANAL'] == 'NO APLICA')
        ]

        # Total real y ppto de merch admon
        sum_merch_admon = (df_merch_admon[['VALOR REAL', 'VALOR PPTO']].sum())/1000000

        # DATAFRAME DE PORCENTAJES EXCLUSIVOS PARA MERCH ADMON
        porcj_exclusivo_merch_admon = {
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'],
            'PORCENTAJE SEGMENTOS': [0.30, 0.30, 0.14, 0.26]}
        df_porcj_exclusivo_merch_admon = pd.DataFrame(porcj_exclusivo_merch_admon)

        # Operar el valor real y ppto por el porcentaje
        df_porcj_exclusivo_merch_admon['VALOR REAL CALCULADO'] = (
            sum_merch_admon['VALOR REAL'] * df_porcj_exclusivo_merch_admon['PORCENTAJE SEGMENTOS']
        )
        df_porcj_exclusivo_merch_admon['VALOR PPTO CALCULADO'] = (
            sum_merch_admon['VALOR PPTO'] * df_porcj_exclusivo_merch_admon['PORCENTAJE SEGMENTOS']
        )

        # DATAFRAME COMBINADO DE MP Y EXP. NO LOCALES PARA OPERAR CON LOS DRIVERS DE MERCH ADMON 
        df_merged_mp_enl = pd.merge(
            df_drivers_merch_admon, df_porcj_exclusivo_merch_admon, on='SEGMENTO'
        )

        # Operar el valor calculado con los drivers
        df_merged_mp_enl.loc[df_merged_mp_enl['SEGMENTO'].isin(
            ['MARCAS PROPIAS', 'EXP. NO LOCALES']), 'VALOR REAL RESULTANTE'] = (
                df_merged_mp_enl['PORCENTAJE DRIVER'] * df_merged_mp_enl['VALOR REAL CALCULADO']
        )
        df_merged_mp_enl.loc[df_merged_mp_enl['SEGMENTO'].isin(
            ['MARCAS PROPIAS', 'EXP. NO LOCALES']), 'VALOR PPTO RESULTANTE'] = (
                df_merged_mp_enl['PORCENTAJE DRIVER'] * df_merged_mp_enl['VALOR PPTO CALCULADO']
        )

        # Total real y ppto de cada Marca de Marcas propias
        sums_mp = []
        for marca in marcas_mp:
            suma = df_vyc_mp[
                (df_vyc_mp['MARCA'] == marca) & 
                (df_vyc_mp['CONCEPTO'] == 'INGRESO NETO')
            ][['VALOR REAL', 'VALOR PPTO']].sum()
            sums_mp.append([marca, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_total_marca_mp = pd.DataFrame(sums_mp, columns=['MARCA', 'VALOR REAL', 'VALOR PPTO'])

        # Total real y ppto de cada canal y marca
        sum_canal_mp = []
        for marca in marcas_mp:
            for canal in canales_mp:
                suma = df_vyc_mp[
                    (df_vyc_mp['MARCA'] == marca) & 
                    (df_vyc_mp['CANAL'] == canal) &
                    (df_vyc_mp['CONCEPTO'] == 'INGRESO NETO')
                ][['VALOR REAL', 'VALOR PPTO']].sum()
                sum_canal_mp.append([marca, canal, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_sum_canal_mp = pd.DataFrame(sum_canal_mp, columns=['MARCA', 'CANAL', 'VALOR REAL', 'VALOR PPTO'])

        # Dataframe combinado para obtener los porcentajes 
        df_porcj_marca_canal_mp = pd.merge(df_sum_canal_mp, df_total_marca_mp, on='MARCA')

        # Obtener porcentajes real y ppto
        df_porcj_marca_canal_mp['PORCENTAJE REAL'] = (
            df_porcj_marca_canal_mp['VALOR REAL_x'] / df_porcj_marca_canal_mp['VALOR REAL_y']
        )
        df_porcj_marca_canal_mp['PORCENTAJE PPTO'] = (
            df_porcj_marca_canal_mp['VALOR PPTO_x'] / df_porcj_marca_canal_mp['VALOR PPTO_y']
        )

        # Total real y ppto de cada Marca de Marcas propias
        sums_enl = []
        for marca in marcas_enl:
            suma = df_vyc_enl[
                (df_vyc_enl['MARCA'] == marca) & 
                (df_vyc_enl['CONCEPTO'] == 'INGRESO NETO')
            ][['VALOR REAL', 'VALOR PPTO']].sum()
            sums_enl.append([marca, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_total_marca_enl = pd.DataFrame(sums_enl, columns=['MARCA', 'VALOR REAL', 'VALOR PPTO'])

        # Total real y ppto de cada canal y marca
        sum_canal_enl = []
        for marca in marcas_enl:
            for canal in canales_enl:
                suma = df_vyc_enl[
                    (df_vyc_enl['MARCA'] == marca) & 
                    (df_vyc_enl['CANAL'] == canal) &
                    (df_vyc_enl['CONCEPTO'] == 'INGRESO NETO')
                ][['VALOR REAL', 'VALOR PPTO']].sum()
                sum_canal_enl.append([marca, canal, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_sum_canal_enl = pd.DataFrame(sum_canal_enl, columns=['MARCA', 'CANAL', 'VALOR REAL', 'VALOR PPTO'])

        # Dataframe combinado para obtener los porcentajes 
        df_porcj_marca_canal_enl = pd.merge(df_sum_canal_enl, df_total_marca_enl, on='MARCA')

        # Obtener porcentajes real y ppto
        df_porcj_marca_canal_enl['PORCENTAJE REAL'] = (
            df_porcj_marca_canal_enl['VALOR REAL_x'] / df_porcj_marca_canal_enl['VALOR REAL_y']
        )
        df_porcj_marca_canal_enl['PORCENTAJE PPTO'] = (
            df_porcj_marca_canal_enl['VALOR PPTO_x'] / df_porcj_marca_canal_enl['VALOR PPTO_y']
        )

        # DATAFRAME COMBINADO DE LOS DATAFRAME COMBINADOS PARA SACAR EL RESULTADO FINAL DE MP Y ENL
        df_merged_mp_enl_valor = df_merged_mp_enl[
            ['SEGMENTO', 'MARCA', 'VALOR REAL RESULTANTE', 'VALOR PPTO RESULTANTE']
        ]

        df_resultado_final_mp = pd.merge(
            df_porcj_marca_canal_mp,df_merged_mp_enl_valor, on='MARCA'
        )
        df_resultado_final_mp['VALOR REAL FINAL'] = (
            df_resultado_final_mp['VALOR REAL RESULTANTE'] * df_resultado_final_mp['PORCENTAJE REAL']
        )
        df_resultado_final_mp['VALOR PPTO FINAL'] = (
            df_resultado_final_mp['VALOR PPTO RESULTANTE'] * df_resultado_final_mp['PORCENTAJE PPTO']
        )

        df_resultado_final_enl = pd.merge(
            df_porcj_marca_canal_enl,df_merged_mp_enl_valor, on='MARCA'
        )
        df_resultado_final_enl['VALOR REAL FINAL'] = (
            df_resultado_final_enl['VALOR REAL RESULTANTE'] * df_resultado_final_enl['PORCENTAJE REAL']
        )
        df_resultado_final_enl['VALOR PPTO FINAL'] = (
            df_resultado_final_enl['VALOR PPTO RESULTANTE'] * df_resultado_final_enl['PORCENTAJE PPTO']
        )

        # DATAFRAME COMBINADO DE LOS DATAFRAME COMBINADOS CON VALORES FINAL DE MP Y EXP. NO LOCALES
        df_resultado_final_mp_enl = pd.concat(
            [df_resultado_final_mp, df_resultado_final_enl], ignore_index=True
        )

        # ASIGNAR EL DRIVER AL DF FINAL DE MP Y ENL
        df_resultado_final_mp_enl['DRIVER'] = 'MERCHANDISING ADMON'

        # DATAFRAME COMBINADO FINAL DE MP Y EXP. NO LOCALES
        df_merged_final_mp_enl = df_resultado_final_mp_enl[['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL FINAL', 'VALOR PPTO FINAL']]

        df_merged_final_mp_enl = df_merged_final_mp_enl.copy()
        column_order = ['MARCA', 'CANAL',  'DRIVER', 'VALOR REAL FINAL', 'VALOR PPTO FINAL']
        df_merged_final_mp_enl = df_merged_final_mp_enl[column_order]
        df_merged_final_mp_enl.reset_index(drop=True, inplace=True)

        # DATAFRAMES DE VENTA Y COSTO DE DO Y EXP. LOCALES 
        do = df_vyc_do[['VALOR REAL', 'VALOR PPTO']].sum()
        el = df_vyc_el[['VALOR REAL', 'VALOR PPTO']].sum()

        # PORCENTAJES ESPECIALES DE DO Y EXP. LOCALES PARA OPERAR 
        v_calculado_el_real = df_porcj_exclusivo_merch_admon.loc[
            df_porcj_exclusivo_merch_admon['SEGMENTO'] == 'EXP. LOCALES', 'VALOR REAL CALCULADO'
        ].values[0]
        v_calculado_el_ppto = df_porcj_exclusivo_merch_admon.loc[
            df_porcj_exclusivo_merch_admon['SEGMENTO'] == 'EXP. LOCALES', 'VALOR PPTO CALCULADO'
        ].values[0]

        v_calculado_el_real = v_calculado_el_real/(el['VALOR REAL'])
        v_calculado_el_ppto = v_calculado_el_ppto/(el['VALOR PPTO'])

        v_calculado_do_real = df_porcj_exclusivo_merch_admon.loc[
            df_porcj_exclusivo_merch_admon['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR REAL CALCULADO'
        ].values[0]
        v_calculado_do_ppto = df_porcj_exclusivo_merch_admon.loc[
            df_porcj_exclusivo_merch_admon['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR PPTO CALCULADO'
        ].values[0]

        v_calculado_do_real = v_calculado_do_real/(do['VALOR REAL'])
        v_calculado_do_ppto = v_calculado_do_ppto/(do['VALOR PPTO'])

        # DATAFRAME DONDE SE AGREGAN LOS PORCENTAJES ESPECIALES
        new_rows = [
            {'SEGMENTO': 'EXP. LOCALES',
            'VALOR REAL DRIVER': v_calculado_el_real,
            'VALOR PPTO DRIVER': v_calculado_el_ppto,
            },
            {'SEGMENTO': 'DEMAND OWNERS',
            'VALOR REAL DRIVER': v_calculado_do_real,
            'VALOR PPTO DRIVER': v_calculado_do_ppto,
            }
        ]
        new_df = pd.DataFrame(new_rows)

        # DATAFRAME COMBINADO PARA OPERAR LOS PORCENTAJES
        combined_df = pd.merge(new_df, df_porcj_exclusivo_merch_admon, on='SEGMENTO')

        # DATAFRAME COMBINADO DE EXP. LOCALES 
        df_merged_el = pd.merge(df_vyc_el, combined_df, on='SEGMENTO', how='left')

        df_merged_el['VALOR REAL RESULTANTE'] = df_merged_el['VALOR REAL'] * df_merged_el['VALOR REAL DRIVER']
        df_merged_el['VALOR PPTO RESULTANTE'] = df_merged_el['VALOR PPTO'] * df_merged_el['VALOR PPTO DRIVER']
        df_merged_el['DRIVER'] = 'MERCHANDISING ADMON'

        # DATAFRAME COMBINADO DE DEMAND OWNERS
        df_merged_do = pd.merge(df_vyc_do, combined_df, on='SEGMENTO', how='left')
        df_merged_do['VALOR REAL RESULTANTE'] = df_merged_do['VALOR REAL'] * df_merged_do['VALOR REAL DRIVER']
        df_merged_do['VALOR PPTO RESULTANTE'] = df_merged_do['VALOR PPTO'] * df_merged_do['VALOR PPTO DRIVER']
        df_merged_do['DRIVER'] = 'MERCHANDISING ADMON'

        # FILTRAR LOS DATAFRAME COMBINADOS PARA UNIRLOS
        filtro_do = df_merged_do[['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL RESULTANTE', 'VALOR PPTO RESULTANTE']]
        filtro_el = df_merged_el[['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL RESULTANTE', 'VALOR PPTO RESULTANTE']]

        # DATAFRAME COMBINADO FINAL DE DO Y EXP. LOCALES
        df_merged_do_el = pd.concat([filtro_do, filtro_el])
        column_order = ['MARCA', 'CANAL', 'DRIVER', 'VALOR REAL RESULTANTE', 'VALOR PPTO RESULTANTE']
        df_merged_do_el = df_merged_do_el[column_order]
        df_merged_do_el.reset_index(drop=True, inplace=True)

        # DATAFRAME FINAL MERCHANDISING ADMON
        df_final_merch_admon = pd.concat(
            [df_merged_final_mp_enl, df_merged_do_el], ignore_index=True
        )

        df_final_merch_admon['VALOR REAL'] = df_final_merch_admon['VALOR REAL FINAL'].fillna(
            df_final_merch_admon['VALOR REAL RESULTANTE']
        )
        df_final_merch_admon['VALOR PPTO'] = df_final_merch_admon['VALOR PPTO FINAL'].fillna(
            df_final_merch_admon['VALOR PPTO RESULTANTE']
        )

        df_final_merch_admon.drop(columns=[
            'VALOR REAL FINAL', 'VALOR PPTO FINAL', 'VALOR REAL RESULTANTE', 'VALOR PPTO RESULTANTE'
        ], inplace=True)
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

        # DATAFRAME FINAL FINAL MERCHANDISING ADMON 'OK'
        df_bd_directos_merch_admon = pd.DataFrame(columns=['CANAL', 'MARCA', 'DRIVER'])
        df_bd_directos_merch_admon = pd.concat(
            [df_bd_directos_merch_admon, df_final_merch_admon[columns_to_add]], 
            ignore_index=True
        )


        # =========================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'INVERSION EN MERCHANDISING' * #
        # =========================================================== #

        # Dataframe 1 filtrado para Inversion en merchandising
        df_inv_merch =  df_edg[
            (df_edg['CONCEPTO'] == 'INVERSIÓN EN MERCHANDISING') &  
            (df_edg['CANAL'] != 'NO APLICA')
        ]

        # Dataframe con el total real y ppto por canal
        df_valores_inv_merch = df_inv_merch.groupby(
            'CANAL'
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_valores_inv_merch.columns = ['CANAL', 'VALOR REAL', 'VALOR PPTO']
        df_valores_inv_merch['VALOR REAL'] = df_valores_inv_merch['VALOR REAL']/1000000
        df_valores_inv_merch['VALOR PPTO'] = df_valores_inv_merch['VALOR PPTO']/1000000

        # Total real y ppto de cada Canal
        sums_canales = []
        for canal in canales_general:
            suma = df_vyc[
                (df_vyc['CANAL'] == canal) & 
                (df_vyc['CONCEPTO'] == 'INGRESO NETO')
            ][['VALOR REAL', 'VALOR PPTO']].sum()
            sums_canales.append([canal, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_total_canal = pd.DataFrame(sums_canales, columns=['CANAL', 'VALOR REAL', 'VALOR PPTO'])

        # Total real y ppto de cada canal y marca
        sum_marca =[]
        for marca in marcas_general:
            for canal in canales_general:
                suma = df_vyc[
                    (df_vyc['CANAL'] == canal) & 
                    (df_vyc['MARCA'] == marca) & 
                    (df_vyc['CONCEPTO'] == 'INGRESO NETO')
                ][['VALOR REAL', 'VALOR PPTO']].sum()
                sum_marca.append([canal, marca, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_sum_marca_canal = pd.DataFrame(
            sum_marca, columns=['CANAL', 'MARCA', 'VALOR REAL', 'VALOR PPTO']
        )

        # Dataframe combinado para obtener los porcentajes 
        df_merged_marca_canal = pd.merge(df_sum_marca_canal, df_total_canal, on='CANAL')

        # Filtrar el dataframe porque solo necesitamos ecommerce
        df_merged_marca_canal = df_merged_marca_canal[df_merged_marca_canal['CANAL'] == 'E-COMMERCE']

        # Obtener porcentajes real y ppto
        df_merged_marca_canal['PORCENTAJE REAL'] = (
            df_merged_marca_canal['VALOR REAL_x'] / df_merged_marca_canal['VALOR REAL_y']
        )
        df_merged_marca_canal['PORCENTAJE PPTO'] = (
            df_merged_marca_canal['VALOR PPTO_x'] / df_merged_marca_canal['VALOR PPTO_y']
        )

        # Adjuntar en el dataframe el valor a repartir
        df_merged_marca_canal['VALOR REAL'] = df_valores_inv_merch['VALOR REAL'].iloc[0]
        df_merged_marca_canal['VALOR PPTO'] = df_valores_inv_merch['VALOR PPTO'].iloc[0]

        # Obtener el valor repartido
        df_merged_marca_canal['VALOR REAL'] = (
            df_merged_marca_canal['VALOR REAL'] * df_merged_marca_canal['PORCENTAJE REAL']
        )
        df_merged_marca_canal['VALOR PPTO'] = (
            df_merged_marca_canal['VALOR PPTO'] * df_merged_marca_canal['PORCENTAJE PPTO']
        )

        # Añadir el driver y dejar solo columnas necesarias
        df_merged_marca_canal['DRIVER'] = 'INVERSIÓN EN MERCHANDISING'
        df_merged_marca_canal = df_merged_marca_canal.copy()
        column_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        df_merged_marca_canal = df_merged_marca_canal[column_to_add]

        # Dataframe 2 filtrado para Inversion en merchandising
        df2_inv_merch =  df_edg[
            (df_edg['CONCEPTO'] == 'INVERSIÓN EN MERCHANDISING') & 
            (df_edg['MARCA'] != 'NO APLICA')
        ]

        # Dataframe con el total real y ppto por marca
        df2_valores_inv_merch = df2_inv_merch.groupby(
            'MARCA'
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df2_valores_inv_merch.columns = ['MARCA', 'VALOR REAL', 'VALOR PPTO']

        # Dataframes combinado para operar con los drivers
        df_merged_real = df_drivers_real[df_drivers_real['DRIVER'] == 'INVERSIÓN EN MERCHANDISING'].merge(
            df2_valores_inv_merch, on='MARCA'
        )
        df_merged_ppto = df_drivers_ppto[df_drivers_ppto['DRIVER'] == 'INVERSIÓN EN MERCHANDISING'].merge(
            df2_valores_inv_merch, on='MARCA'
        )

        # Operar los dataframes combinados
        df_merged_real['VALOR REAL'] = (
            df_merged_real['VALOR REAL'] * df_merged_real['VALOR PORCENTAJE']
        )/1000000
        df_merged_real['VALOR PPTO'] = (
            df_merged_ppto['VALOR PPTO'] * df_merged_ppto['VALOR PORCENTAJE']
        )/1000000

        df_merged_real = df_merged_real.drop(columns=['AÑO', 'VALOR PORCENTAJE'])

        # Concatenar el df combinado al df bd final
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        df_bd_directos_inv_merch = pd.concat(
            [df_merged_real[columns_to_add], df_merged_marca_canal], ignore_index=True
        )
        del df_merged_real, df_merged_ppto

        # Eliminar filas donde el real y el ppto sean cero absoluto
        df_bd_directos_inv_merch = df_bd_directos_inv_merch[
            (~((df_bd_directos_inv_merch['VALOR REAL'] == 0.0) & (df_bd_directos_inv_merch['VALOR PPTO'] == 0.0)))
        ]

        # ============================================== #
        # * CONSOLIDAR TODOS LOS RUBROS EN UNA SOLA BD * #
        # ============================================== #

        dataframes_bd_gastos_directos = [
            df_bd_directos_inv_merc,
            df_bd_directos_admin_marca,
            df_bd_directos_merch_admon,
            df_bd_directos_inv_merch
        ]

        # Dataframe final de la base de datos directos
        df_bd_gastos_directos = pd.DataFrame(
            columns=['MARCA', 'CANAL', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        )
        df_bd_gastos_directos = pd.concat(dataframes_bd_gastos_directos, ignore_index=True)

        # Insertar fecha
        df_bd_gastos_directos.insert(0, 'AÑO', año)
        df_bd_gastos_directos.insert(1, 'MES', mes_letras)

        # Insertar segmentos
        df_bd_gastos_directos = utils.insert_segmentos(df_bd_gastos_directos)

        # Organizar columnas
        df_bd_gastos_directos = df_bd_gastos_directos[
            ['AÑO', 'MES', 'MARCA', 'SEGMENTO',  'CANAL', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        ]

        # Exportar base de datos mensual
        sub_dirs_final_db = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
            'GASTOS'
        ]
        path_bd_gastos_directos = os.path.join(
            base_dir, *sub_dirs_final_db, f'BD_GASTOS_DIRECTOS_{mes}.{año}.xlsx'
        )
        df_bd_gastos_directos.to_excel(path_bd_gastos_directos, index=False)

        return df_bd_gastos_directos
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()