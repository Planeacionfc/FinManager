import pandas as pd
import os
from controller.cmiController import vycController
from scripts.utils import utils
from .bd_g_directos import get_bd_gastos_directos


def get_bd_directos(df_edg: pd.DataFrame, año, mes, mes_largo):
    return hoja_bd_directos(df_edg, año, mes, mes_largo)

def hoja_bd_directos(df_edg: pd.DataFrame, año, mes, mes_largo):
    try:
        
        # Ruta a cada base de datos
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
        sub_dirs_regalias = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'REGALIAS'
        ]

        # Ruta a cada base de datos externa
        vyc = os.path.join(base_dir, *sub_dirs_final_vyc, f'bd_venta_y_costo_{mes}.{año}.xlsx')
        drivers = os.path.join(base_dir, *sub_dirs_drivers, f'drivers_directos.xlsx')
        flete_correo_path = os.path.join(base_dir, *sub_dirs_flete, f'flete_correo_{año}.xlsx')
        regalias_path = os.path.join(base_dir, *sub_dirs_regalias, f'regalias_{año}.xlsx')

        # Leer las bases de datos externas
        df_vyc = pd.read_excel(vyc, engine='openpyxl', sheet_name='Sheet1')
        df_drivers = pd.read_excel(drivers)
        df_flete_correo = pd.read_excel(flete_correo_path)
        df_regalias = pd.read_excel(regalias_path, sheet_name='Sheet1')

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

        # Crear dataframe con filtros necesarios
        df_vyc = df_vyc[['CANAL', 'MARCA', 'SEGMENTO', 'CONCEPTO', 'VALOR REAL', 'VALOR PPTO']]
        df_drivers_real = df_drivers[df_drivers['AÑO'] == int(año)]
        df_drivers_ppto = df_drivers[df_drivers['AÑO'] == f"{año} P"]
        df_flete_correo = df_flete_correo[df_flete_correo['MES'] == mes_letras]
        df_regalias = df_regalias[df_regalias['MES'] == mes_letras]

        # Dataframe filtrados de venta y costo
        df_vyc_mp = df_vyc[(df_vyc['SEGMENTO'] == 'MARCAS PROPIAS')]
        df_vyc_enl = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. NO LOCALES')]
        df_vyc_el = df_vyc[(df_vyc['SEGMENTO'] == 'EXP. LOCALES')]
        df_vyc_do = df_vyc[(df_vyc['SEGMENTO'] == 'DEMAND OWNERS')]

        # ============================================ #
        # * TRANSFORMACIÓN DEL RUBRO - 'TRADE ADMON' * #
        # ============================================ #

        # Dataframe filtrado para Trade Admon
        df_trade_admon = df_edg[df_edg['CONCEPTO'] == 'TRADE ADMON']

        # Dataframe con el total real y ppto por canal
        df_real_trade_admon = df_trade_admon.groupby('CANAL')['VALOR REAL'].sum().reset_index()
        df_ppto_trade_admon = df_trade_admon.groupby('CANAL')['VALOR PPTO'].sum().reset_index()

        # Dataframes combinado para operar con los drivers
        df_merged_real = pd.merge(
            df_drivers_real[df_drivers_real['DRIVER'] == 'TRADE ADMON'],
            df_real_trade_admon, on='CANAL'
        )
        df_merged_ppto = pd.merge(
            df_drivers_ppto[df_drivers_ppto['DRIVER'] == 'TRADE ADMON'],
            df_ppto_trade_admon, on='CANAL'
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
        df_bd_directos_trade_admon = pd.concat(
            [df_merged_real[columns_to_add]], ignore_index=True
        )
        del df_merged_real, df_merged_ppto

        # Parte de bienestar para trade admon
        df_bienestar = df_edg[
            (df_edg['NOMBRE CECO'] == 'CAPACITACIÓN RETAIL') |
            (df_edg['NOMBRE CECO'] == 'BIENESTAR RETAIL')
        ]

        # Obtener el total real y ppto
        total_bienestar_real = df_bienestar['VALOR REAL'].sum()
        total_bienestar_ppto = df_bienestar['VALOR PPTO'].sum()

        # Total ventas brutas real y ppto
        ventas_enl_real = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'VENTAS BRUTAS']['VALOR REAL'].sum()
        ventas_enl_ppto = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'VENTAS BRUTAS']['VALOR PPTO'].sum()
        ventas_mp_real = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'VENTAS BRUTAS']['VALOR REAL'].sum()
        ventas_mp_ppto = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'VENTAS BRUTAS']['VALOR PPTO'].sum()

        # Suma de las ventas brutas
        total_mp_enl_real = ventas_enl_real+ventas_mp_real
        total_mp_enl_ppto = ventas_enl_ppto+ventas_mp_ppto

        # Operar y obtener porcentajes real y ppyo
        porcj_enl_real = ventas_enl_real/total_mp_enl_real
        porcj_enl_ppto = ventas_enl_ppto/total_mp_enl_ppto

        porcj_mp_real = ventas_mp_real/total_mp_enl_real
        porcj_mp_ppto = ventas_mp_ppto/total_mp_enl_ppto

        # Operar el total de bienestrar
        valor_enl_real = (porcj_enl_real*total_bienestar_real)/1000000
        valor_enl_ppto = (porcj_enl_real*total_bienestar_ppto)/1000000

        valor_mp_real = (porcj_mp_real*total_bienestar_real)/1000000
        valor_mp_ppto = (porcj_mp_ppto*total_bienestar_ppto)/1000000

        # Total ingreso neto real y ppto
        ingreso_enl_real = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'INGRESO NETO']['VALOR REAL'].sum()
        ingreso_enl_ppto = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'INGRESO NETO']['VALOR PPTO'].sum()
        ingreso_mp_real = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'INGRESO NETO']['VALOR REAL'].sum()
        ingreso_mp_ppto = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'INGRESO NETO']['VALOR PPTO'].sum()

        # Calcular el porcentaje del driver
        porcj_enl_bienestar_real = valor_enl_real/ingreso_enl_real
        porcj_enl_bienestar_ppto = valor_enl_ppto/ingreso_enl_ppto
        porcj_mp_bienestar_real = valor_mp_real/ingreso_mp_real
        porcj_mp_bienestar_ppto = valor_mp_ppto/ingreso_mp_ppto

        # Obtener el ingreso neto de cada marca
        total_ingreso_enl = df_vyc_enl[
            df_vyc_enl['CONCEPTO'] == 'INGRESO NETO'
        ].groupby(['CANAL', 'MARCA'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        total_ingreso_mp = df_vyc_mp[
            df_vyc_mp['CONCEPTO'] == 'INGRESO NETO'
        ].groupby(['CANAL', 'MARCA'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Operar para obtener el valor final de bienestar
        total_ingreso_enl['VALOR REAL'] = (total_ingreso_enl['VALOR REAL'] * porcj_enl_bienestar_real)
        total_ingreso_enl['VALOR PPTO'] = (total_ingreso_enl['VALOR PPTO'] * porcj_enl_bienestar_ppto)
        total_ingreso_mp['VALOR REAL'] = (total_ingreso_mp['VALOR REAL'] * porcj_mp_bienestar_real)
        total_ingreso_mp['VALOR PPTO'] = (total_ingreso_mp['VALOR PPTO'] * porcj_mp_bienestar_ppto)

        # Concatenar ambos segmentos
        total_ingreso_mp_enl = pd.concat(
            [total_ingreso_mp, total_ingreso_enl], ignore_index=True
        )
        total_ingreso_mp_enl['DRIVER'] = 'TRADE ADMON'

        # Concatenar el df de bienestar con el df final
        df_bd_directos_trade_admon = pd.concat(
            [df_bd_directos_trade_admon, total_ingreso_mp_enl],
            ignore_index=True
        )

        # =================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'INVERSION EN TRADE' * #
        # =================================================== #

        # Dataframe filtrado para Inversión en Trade
        df_inv_trade = df_edg[df_edg['CONCEPTO'] ==  'INVERSIÓN EN TRADE']

        # Dataframe con el total real y ppto por marca y canal
        df_inv_trade = df_inv_trade.groupby(['MARCA', 'CANAL'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Añadir valores necesarios para el df final
        df_inv_trade[['VALOR REAL', 'VALOR PPTO']] = df_inv_trade[['VALOR REAL', 'VALOR PPTO']]/1000000
        df_inv_trade['DRIVER'] = 'INVERSIÓN EN TRADE'

        # Concatenar el df combinado al df bd final
        df_bd_directos_inv_trade = pd.concat(
            [df_inv_trade], ignore_index=True
        )

        # ======================================================= #
        # * TRANSFORMACIÓN DEL RUBRO - 'OTROS GASTOS VARIABLES' * #
        # ======================================================= #

        # Dataframe filtrado para Otros Gastos Variables
        df_ogv = df_edg[df_edg['CONCEPTO'] ==  'OTROS GASTOS VARIABLES']
        df_ogv = df_ogv.dropna(subset=['MARCA'])

        # Cambiar marcas del dataframe
        mapa_marcas = {
        'FAMILIA': 'OTROS EXP. LOCALES',
        'LOREAL': 'OTROS EXP. LOCALES',
        'OTROS (CANAL FPT)': 'OTROS EXP. LOCALES',
        'JOYERIA': 'OTROS EXP. LOCALES',

        'AMAZON': 'OTROS CLIENTES DO',
        'CALA': 'OTROS CLIENTES DO',
        }
        df_ogv['MARCA'] = df_ogv['MARCA'].replace(mapa_marcas)

        # Dataframe con el total real y ppto por marca y canal
        df_ogv = df_ogv.groupby(['MARCA', 'CANAL'])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Añadir valores necesarios para el df final
        df_ogv[['VALOR REAL', 'VALOR PPTO']] = df_ogv[['VALOR REAL', 'VALOR PPTO']]/1000000
        df_ogv['DRIVER'] = 'OTROS GASTOS VARIABLES'

        # Concatenar el df combinado al df bd final
        df_bd_directos_otros_g_variables = pd.concat(
            [df_ogv], ignore_index=True
        )

        # ======================================================== #    
        # * TRANSFORMACIÓN DEL RUBRO - 'PROVISION DE INVENTARIO' * #
        # ======================================================== #

        # Esta informacion cae por sap directamente
        controlador = vycController.vycController()
        data = controlador.obtener_datos_sap(año, mes_largo)

        # Tranformar la informaciona  un dataframe
        df_prov_inventario = controlador.transformar_datos_prov_inventario(data)
        df_prov_inventario = df_prov_inventario.rename(
            columns={'CONCEPTO': 'DRIVER'}
        )
        
        # Repatir el canal 'Otros'
        df_prov_inventario = df_prov_inventario[df_prov_inventario['CANAL'] == 'OTROS'] 
        df_valores_filtrados = df_vyc[
            (~df_vyc['CANAL'].isin(['FABRIC PARA TERCEROS', 'E-COMMERCE']))
            & (df_vyc['CONCEPTO'] == 'INGRESO NETO')
        ]
        
        # Dataframe con el total real y ppto por marca
        df_real_prov_inv = df_valores_filtrados.groupby('MARCA')['VALOR REAL'].sum().reset_index()
        df_ppto_prov_inv = df_valores_filtrados.groupby('MARCA')['VALOR PPTO'].sum().reset_index()
        df_real_prov_inv = df_real_prov_inv.rename(columns={'VALOR REAL': 'SUMA TOTAL'})
        df_ppto_prov_inv = df_ppto_prov_inv.rename(columns={'VALOR PPTO': 'SUMA TOTAL'})
        
        # Dataframe con el total real y ppto por marca canal
        df_real_marca_canal = df_valores_filtrados.groupby(
            ['MARCA', 'CANAL']
        )['VALOR REAL'].sum().reset_index()
        df_ppto_marca_canal = df_valores_filtrados.groupby(
            ['MARCA', 'CANAL']
        )['VALOR PPTO'].sum().reset_index()
        df_real_marca_canal = df_real_marca_canal.rename(columns={'VALOR REAL': 'VALOR MARCA'})
        df_ppto_marca_canal = df_ppto_marca_canal.rename(columns={'VALOR PPTO': 'VALOR MARCA'})
        

        # Obtener el porcentaje de marca canal real
        df_porcj_marca_canal_real = pd.merge(df_real_marca_canal, df_real_prov_inv, on='MARCA')
        df_porcj_marca_canal_real['PORCENTAJE'] = (
            df_porcj_marca_canal_real['VALOR MARCA'] / df_porcj_marca_canal_real['SUMA TOTAL']
        )

        # Obtener el porcentaje de marca canal ppto
        df_porcj_marca_canal_ppto = pd.merge(df_ppto_marca_canal, df_ppto_prov_inv, on='MARCA')
        df_porcj_marca_canal_ppto['PORCENTAJE'] = (
            df_porcj_marca_canal_ppto['VALOR MARCA'] / df_porcj_marca_canal_ppto['SUMA TOTAL']
        )

        # Combinar y operar la prov real
        df_prov_inventario_real = pd.merge(
            df_prov_inventario, df_porcj_marca_canal_real, on='MARCA', how='left'
        )
        df_prov_inventario_real['VALOR REAL'] = (
            df_prov_inventario_real['VALOR REAL'] * df_prov_inventario_real['PORCENTAJE']
        )

        # Combinar y operar la prov ppto
        df_prov_inventario_ppto = pd.merge(
            df_prov_inventario, df_porcj_marca_canal_ppto, on='MARCA', how='left'
        )
        df_prov_inventario_ppto['VALOR PPTO'] = (
            df_prov_inventario_ppto['VALOR PPTO'] * df_prov_inventario_ppto['PORCENTAJE']
        )

        # Añadir valor ppto al df final
        df_prov_inventario_real['VALOR PPTO'] = df_prov_inventario_ppto['VALOR PPTO']

        # Concatenar al df bd final los canales repartidos
        columns_to_add = ['CANAL_y', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        df_bd_directos_prov_inv = pd.concat(
            [df_prov_inventario_real[columns_to_add]],
            ignore_index=True
        )
        df_bd_directos_prov_inv = df_bd_directos_prov_inv.rename(columns={'CANAL_y': 'CANAL'})

        # Canal'FPT' Dataframe filtrado para Inventarios
        df_prov_inventario_fpt = df_edg[
            (df_edg['CONCEPTO'] == 'INVENTARIOS') & (df_edg['NEGOCIO'] == 'FPT')
        ]
        df_prov_inventario_fpt = df_prov_inventario_fpt.copy()

        # Añadir valore necesarios
        df_prov_inventario_fpt['DRIVER'] = 'PROVISIÓN DE INVENTARIO'
        df_prov_inventario_fpt['VALOR REAL'] = df_prov_inventario_fpt['VALOR REAL']/1000000
        df_prov_inventario_fpt['VALOR PPTO'] = df_prov_inventario_fpt['VALOR PPTO']/1000000
        df_prov_inventario = df_prov_inventario_fpt[
            ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        ]
        
        # Concatenar al df bd final el canal fpt
        df_bd_directos_prov_inv = pd.concat(
            [df_bd_directos_prov_inv, df_prov_inventario],
            ignore_index=True
        )

        # Cambiar nombres de marcas
        mapa_marcas = {
            'FAMILIA': 'OTROS EXP. LOCALES', 
            'AMAZON': 'OTROS CLIENTES DO', 
        }
        df_bd_directos_prov_inv['MARCA'] = df_bd_directos_prov_inv['MARCA'].replace(mapa_marcas)

        # ==================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'ICA' * #
        # ==================================== #

        # Dataframe filtrado para ICA
        df_ica = df_edg[['CECO', 'NOMBRE CECO', 'CUENTA', 'VALOR REAL', 'VALOR PPTO']]
        df_ica = df_ica[df_ica['CUENTA'] == 'SUB. IND & CCIO']

        # Cambiar nombre de cecos
        mapa_segmentos = {
            'ACTIVACION DEMANDA': 'MARCAS PROPIAS',
            'DEMAND OWNER': 'DEMAND OWNERS', 
            'EXPERTOS LOCALES': 'EXP. LOCALES',
            'EXPERT. NO LOCALES': 'EXP. NO LOCALES'
        }
        df_ica['NOMBRE CECO'] = df_ica['NOMBRE CECO'].replace(mapa_segmentos)

        # Obtener el dataframe total real y ppto de cada segmento
        sums_segmentos = []
        for segmento in segmentos:
            suma = df_vyc[
                (df_vyc['SEGMENTO'] == segmento) & 
                (df_vyc['CONCEPTO'] == 'INGRESO NETO')
            ][['VALOR REAL', 'VALOR PPTO']].sum()
            sums_segmentos.append([segmento, suma['VALOR REAL'], suma['VALOR PPTO']])
        df_sum_segmento = pd.DataFrame(
            sums_segmentos, columns=['SEGMENTO', 'VALOR REAL', 'VALOR PPTO']
        )

        # Dataframe con el valor por segmento de cada marca y canal
        df_totales_segmento = (df_vyc[df_vyc['CONCEPTO'] == 'INGRESO NETO']).groupby(
            ['SEGMENTO', 'MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Dataframe combinado para obtener los porcentajes exclusivos de ica
        df_porcj_ica = pd.merge(
            df_ica, df_sum_segmento, left_on='NOMBRE CECO', right_on='SEGMENTO'
        )
        df_porcj_ica['PORCENTAJE REAL'] = (
            df_porcj_ica['VALOR REAL_x'] / df_porcj_ica['VALOR REAL_y']
        )/1000000
        df_porcj_ica['PORCENTAJE PPTO'] = (
            df_porcj_ica['VALOR PPTO_x'] / df_porcj_ica['VALOR PPTO_y']
        )/1000000

        # Dataframe combinado para operar el porcentaje
        df_merged = pd.merge(
            df_totales_segmento, df_porcj_ica[['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']],
            on='SEGMENTO', how='left'
        )
        df_merged['VALOR REAL'] = df_merged['VALOR REAL'] * df_merged['PORCENTAJE REAL']
        df_merged['VALOR PPTO'] = df_merged['VALOR PPTO'] * df_merged['PORCENTAJE PPTO']
        df_merged['DRIVER'] = 'ICA'

        # Eliminar columnas inncesarias
        df_merged = df_merged.drop(
            columns=['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']
        )

        # Concatenar el df combinado al df bd final
        df_bd_directos_ica = pd.concat(
            [df_merged], ignore_index=True
        )
        del df_merged

        # ======================================= #
        # * TRANSFORMACIÓN DEL RUBRO - '4X1000' * #
        # ======================================= #

        # Dataframe filtrado para 4X1000
        df_4x1000 = df_edg[['CECO', 'NOMBRE CECO', 'CUENTA', 'VALOR REAL', 'VALOR PPTO']]
        df_4x1000 = df_4x1000[df_4x1000['CUENTA'] == 'SUB. 4 X 1000']

        # Cambiar nombre de cecos 
        df_4x1000['NOMBRE CECO'] = df_4x1000['NOMBRE CECO'].replace(mapa_segmentos)

        # Dataframe combinado para obtener los porcentajes exclusivos de 4x1000
        df_porcj_4x1000 = pd.merge(
            df_4x1000, df_sum_segmento, left_on='NOMBRE CECO', right_on='SEGMENTO'
        )
        df_porcj_4x1000['PORCENTAJE REAL'] = (
            df_porcj_4x1000['VALOR REAL_x'] / df_porcj_4x1000['VALOR REAL_y']
        )/1000000
        df_porcj_4x1000['PORCENTAJE PPTO'] = (
            df_porcj_4x1000['VALOR PPTO_x'] / df_porcj_4x1000['VALOR PPTO_y']
        )/1000000

        # Dataframe combinado para operar el porcentaje
        df_merged = pd.merge(
            df_totales_segmento, df_porcj_4x1000[['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']],
            on='SEGMENTO', how='left'
        )
        df_merged['VALOR REAL'] = df_merged['VALOR REAL'] * df_merged['PORCENTAJE REAL']
        df_merged['VALOR PPTO'] = df_merged['VALOR PPTO'] * df_merged['PORCENTAJE PPTO']
        df_merged['DRIVER'] = '4X1000'

        # Eliminar columnas inncesarias
        df_merged = df_merged.drop(
            columns=['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']
        )

        # Concatenar el df combinado al df bd final
        df_bd_directos_4X1000 = pd.concat(
            [df_merged], ignore_index=True
        )
        del df_merged

        # ==================================================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS DE PERSONAL (NIÑAS) MERCADEO' * #
        # ==================================================================== #

        # Dataframe filtrado para Gasto de Personal (Niñas) Mercadeo
        df_niñas_mcdo = df_edg[df_edg['CONCEPTO'] ==  'GASTOS DE PERSONAL (NIÑAS) MERCADEO']

        # Dataframe con el total real y ppto por marca y canal
        df_niñas_mcdo = df_niñas_mcdo.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        df_niñas_mcdo[['VALOR REAL', 'VALOR PPTO']] = df_niñas_mcdo[['VALOR REAL', 'VALOR PPTO']]/1000000
        df_niñas_mcdo['DRIVER'] = 'GASTOS DE PERSONAL (NIÑAS) MERCADEO'

        # Concatenar el df combinado al df bd final
        df_bd_directos_niñas_mercadeo = pd.concat(
            [df_niñas_mcdo], ignore_index=True
        )

        # ================================================================= #
        # * TRANSFORMACIÓN DEL RUBRO - 'GASTOS DE PERSONAL (NIÑAS) TRADE' * #
        # ================================================================= #

        # Dataframe filtrado para Gasto de Personal (Niñas) Mercadeo
        df_niñas_trade = df_edg[df_edg['CONCEPTO'] ==  'GASTOS DE PERSONAL (NIÑAS) TRADE']

        # Dataframe con el total real y ppto por marca y canal
        df_niñas_trade = df_niñas_trade.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        df_niñas_trade[['VALOR REAL', 'VALOR PPTO']] = df_niñas_trade[['VALOR REAL', 'VALOR PPTO']]/1000000
        df_niñas_trade['DRIVER'] = 'GASTOS DE PERSONAL (NIÑAS) TRADE'

        # Concatenar el df combinado al df bd final
        df_bd_directos_niñas_trade = pd.concat(
            [df_niñas_trade], ignore_index=True
        )
        
        # ============================================ #
        # * TRANSFORMACIÓN DEL RUBRO - 'VP MERCADEO' * #
        # ============================================ #

        # Funcion para filtrar cecos y obtener la suma 
        def calcular_suma(df, nombre_ceco):
            df_filtrado = df[
                (df['NOMBRE CECO'] == nombre_ceco) & 
                (df['CLASE DE COSTO'].astype(str).str.startswith('52')) & 
                (df['CUENTA'] != 'OTROS IMPUESTOS')
            ]
            return df_filtrado[['VALOR REAL', 'VALOR PPTO']].sum()

        # Retorna un df con valores dado porcentajes exclusivos
        def calcular_valores_exclusivos(segmentos, porcentajes, sum_valores):
            df = pd.DataFrame({
                'SEGMENTO': segmentos,
                'PORCENTAJE': porcentajes
            })
            df['VALOR REAL CALCULADO'] = (sum_valores['VALOR REAL'] * df['PORCENTAJE'])
            df['VALOR PPTO CALCULADO'] = (sum_valores['VALOR PPTO'] * df['PORCENTAJE'])
            return df

        # Suma de valores para G. MCDEO ADMON, INTELIGENCIA MCDOS y SERVICIO AL CLIENTE
        sum_g_mcdeo_admon = calcular_suma(df_edg, 'G. MCDEO ADMON')
        sum_inteligencia_mcdos = calcular_suma(df_edg, 'INTELIGENCIA MCDOS')
        sum_serv_al_cliente = calcular_suma(df_edg, 'SERVICIO AL CLIENTE')

        # Dataframe de porcentajes exclusivos G. MCDEO ADMON
        porcentajes_g_mcdeo_admon = [0.295, 0.295, 0.137, 0.273]
        df_g_mcdeo_admon = calcular_valores_exclusivos(
            segmentos, porcentajes_g_mcdeo_admon, sum_g_mcdeo_admon
        )

        # Dataframe de porcentajes exclusivos INTELIGENCIA MCDOS
        porcentajes_inteligencia_mcdos = [0.263, 0.313, 0.146, 0.278]
        df_inteligencia_mcdos = calcular_valores_exclusivos(
            segmentos, porcentajes_inteligencia_mcdos, sum_inteligencia_mcdos
        )

        # (Servicio al cliente) Total de ventas brutas de cada segmento 
        ventas_mp = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'VENTAS BRUTAS'][['VALOR REAL', 'VALOR PPTO']].sum()
        ventas_do = df_vyc_do[df_vyc_do['CONCEPTO'] == 'VENTAS BRUTAS'][['VALOR REAL', 'VALOR PPTO']].sum()
        ventas_enl = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'VENTAS BRUTAS'][['VALOR REAL', 'VALOR PPTO']].sum()
        ventas_el = df_vyc_el[df_vyc_el['CONCEPTO'] == 'VENTAS BRUTAS'][['VALOR REAL', 'VALOR PPTO']].sum()

        # Dataframe de venta y costo sin marcas propias
        df_vyc_sin_mp = df_vyc[
            (df_vyc['SEGMENTO'] != 'MARCAS PROPIAS') & 
            (df_vyc['CONCEPTO'] == 'VENTAS BRUTAS')
        ]
        ventas_sin_mp = df_vyc_sin_mp[['VALOR REAL', 'VALOR PPTO']].sum()

        # Dataframe de venta y costo solo exp. locales y marcas propias
        df_vyc_enl_mp = df_vyc[
            ((df_vyc['SEGMENTO'] == 'EXP. NO LOCALES') | (df_vyc['SEGMENTO'] == 'MARCAS PROPIAS')) & 
            (df_vyc['CONCEPTO'] == 'VENTAS BRUTAS')
        ]
        ventas_enl_mp = df_vyc_enl_mp[['VALOR REAL', 'VALOR PPTO']].sum()

        # Operar las ventas para obtener el porcentaje necesario
        porcj_enl_serv_cliente_real = ventas_enl['VALOR REAL']/ventas_enl_mp['VALOR REAL']
        porcj_enl_serv_cliente_ppto = ventas_enl['VALOR PPTO']/ventas_enl_mp['VALOR PPTO']

        porcj_mp_serv_cliente_real = ventas_mp['VALOR REAL']/ventas_enl_mp['VALOR REAL']
        porcj_mp_serv_cliente_ppto = ventas_mp['VALOR PPTO']/ventas_enl_mp['VALOR PPTO']

        # Dataframe de porcentajes exclusivos ppto SERVICIO AL CLIENTE
        porcentajes_serv_cliente_ppto = [porcj_enl_serv_cliente_ppto, porcj_mp_serv_cliente_ppto, 0, 0]
        df_serv_cliente_ppto = calcular_valores_exclusivos(
            segmentos, porcentajes_serv_cliente_ppto, sum_serv_al_cliente
        )

        # Dataframe de porcentajes exclusivos real SERVICIO AL CLIENTE
        porcentajes_serv_cliente_real = [porcj_enl_serv_cliente_real, porcj_mp_serv_cliente_real, 0, 0]
        df_serv_cliente_real = calcular_valores_exclusivos(
            segmentos, porcentajes_serv_cliente_real, sum_serv_al_cliente
        )

        # Combinar servicio al cliente con ambos valores
        df_serv_cliente = pd.merge(
            df_serv_cliente_real[['SEGMENTO', 'VALOR REAL CALCULADO']], 
            df_serv_cliente_ppto[['SEGMENTO', 'VALOR PPTO CALCULADO']],
            on='SEGMENTO', how='left'
        )

        # Dataframe con los valores calculados de cada ceco
        df_valores_vp_mercadeo = pd.concat(
            [df_g_mcdeo_admon, df_serv_cliente, df_inteligencia_mcdos]
        )
        df_valores_vp_mercadeo = (
            df_valores_vp_mercadeo.groupby('SEGMENTO').sum()/1000000
        ).reset_index()

        # Limpiar un poco el df
        df_valores_vp_mercadeo['VALOR REAL'] = df_valores_vp_mercadeo['VALOR REAL CALCULADO']
        df_valores_vp_mercadeo['VALOR PPTO'] = df_valores_vp_mercadeo['VALOR PPTO CALCULADO']
        df_valores_vp_mercadeo = df_valores_vp_mercadeo.drop(
            columns=['VALOR REAL CALCULADO', 'VALOR PPTO CALCULADO', 'PORCENTAJE']
        )

        # Total de ingreso neto de cada segmento 
        ingreso_mp = df_vyc_mp[df_vyc_mp['CONCEPTO'] == 'INGRESO NETO'][['VALOR REAL', 'VALOR PPTO']].sum()
        ingreso_do = df_vyc_do[df_vyc_do['CONCEPTO'] == 'INGRESO NETO'][['VALOR REAL', 'VALOR PPTO']].sum()
        ingreso_enl = df_vyc_enl[df_vyc_enl['CONCEPTO'] == 'INGRESO NETO'][['VALOR REAL', 'VALOR PPTO']].sum()
        ingreso_el = df_vyc_el[df_vyc_el['CONCEPTO'] == 'INGRESO NETO'][['VALOR REAL', 'VALOR PPTO']].sum()


        # Tomar el valor de cada segmento y sacar el porcentaje con ingreso neto
        porcj_enl_real = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'EXP. NO LOCALES', 'VALOR REAL'
        ].values[0] / ingreso_enl['VALOR REAL']
        porcj_enl_ppto = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'EXP. NO LOCALES', 'VALOR PPTO'
        ].values[0] / ingreso_enl['VALOR PPTO']

        porcj_mp_real = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'MARCAS PROPIAS', 'VALOR REAL'
        ].values[0] / ingreso_mp['VALOR REAL']
        porcj_mp_ppto = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'MARCAS PROPIAS', 'VALOR PPTO'
        ].values[0] / ingreso_mp['VALOR PPTO']

        porcj_do_real = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR REAL'
        ].values[0] / ingreso_do['VALOR REAL']
        porcj_do_ppto = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR PPTO'
        ].values[0] / ingreso_do['VALOR PPTO']

        porcj_el_real = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'EXP. LOCALES', 'VALOR REAL'
        ].values[0] / ingreso_el['VALOR REAL']
        porcj_el_ppto = df_valores_vp_mercadeo.loc[
            df_valores_vp_mercadeo['SEGMENTO'] == 'EXP. LOCALES', 'VALOR PPTO'
        ].values[0] / ingreso_el['VALOR PPTO']

        # Dataframe de porcentajes real y ppto de VP MERCADEO 
        porcj_exclusivo_mercadeo_real = {
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'],
            'PORCENTAJE': [porcj_enl_real, porcj_mp_real, porcj_el_real, porcj_do_real]
        }
        df_porcj_exclusivo_mercadeo_real = pd.DataFrame(porcj_exclusivo_mercadeo_real)
        porcj_exclusivo_mercadeo_ppto = {
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'],
            'PORCENTAJE': [porcj_enl_ppto, porcj_mp_ppto, porcj_el_ppto, porcj_do_ppto]
        }
        df_porcj_exclusivo_mercadeo_ppto = pd.DataFrame(porcj_exclusivo_mercadeo_ppto)

        # Combinar los dataframe de porcentajes
        df_porcj_exclusivo_mercadeo_real['PORCENTAJE REAL'] = df_porcj_exclusivo_mercadeo_real['PORCENTAJE']
        df_porcj_exclusivo_mercadeo_real['PORCENTAJE PPTO'] = df_porcj_exclusivo_mercadeo_ppto['PORCENTAJE']
        df_porcj_exclusivo_mercadeo_real = df_porcj_exclusivo_mercadeo_real.drop(columns='PORCENTAJE')

        # Dataframe con el valor por segmento de cada marca y canal
        df_totales_segmento = (df_vyc[df_vyc['CONCEPTO'] == 'INGRESO NETO']).groupby(
            ['SEGMENTO', 'MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Dataframe combinado para operar el porcentaje
        df_merged = pd.merge(
            df_totales_segmento, 
            df_porcj_exclusivo_mercadeo_real[['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']],
            on='SEGMENTO', how='left'
        )
        df_merged['VALOR REAL'] = df_merged['VALOR REAL'] * df_merged['PORCENTAJE REAL']
        df_merged['VALOR PPTO'] = df_merged['VALOR PPTO'] * df_merged['PORCENTAJE PPTO']
        df_merged['DRIVER'] = 'VP MERCADEO'

        # Eliminar filas inncesearias
        df_merged = df_merged.drop(columns=['PORCENTAJE REAL', 'PORCENTAJE PPTO'])

        # Concatenar el df combinado al df bd final
        df_bd_directos_vp_mercadeo = pd.concat(
            [df_merged], ignore_index=True
        )
        del df_merged
        
        # ============================================= #
        # * TRANSFORMACIÓN DEL RUBRO - 'VP COMERCIAL' * #
        # ============================================= #

        # Suma de valores para DESARROLLO D NEGOCIO, DEMAND OWNERS y EXP. NO LOCALES
        sum_ddn = calcular_suma(df_edg, 'DESARROLLO D NEGOCIO')
        sum_do = calcular_suma(df_edg, 'ACTIVACION DEMANDA')
        sum_enl = calcular_suma(df_edg, 'EXPERT. NO LOCALES')

        # Operar para obtener el porcentaje de ddn
        porcj_enl_ddn_real = ventas_enl['VALOR REAL'] / ventas_sin_mp['VALOR REAL']
        porcj_enl_ddn_ppto = ventas_enl['VALOR PPTO'] / ventas_sin_mp['VALOR PPTO']

        porcj_el_ddn_real = ventas_el['VALOR REAL'] / ventas_sin_mp['VALOR REAL']
        porcj_el_ddn_ppto = ventas_el['VALOR PPTO'] / ventas_sin_mp['VALOR PPTO']

        porcj_do_ddn_real =  ventas_do['VALOR REAL'] / ventas_sin_mp['VALOR REAL']
        porcj_do_ddn_ppto =  ventas_do['VALOR PPTO'] / ventas_sin_mp['VALOR PPTO']

        # Operar para obtener el porcentaje de enl
        porcj_enl_enl_real = ventas_enl['VALOR REAL'] / ventas_enl_mp['VALOR REAL']
        porcj_enl_enl_ppto = ventas_enl['VALOR PPTO'] / ventas_enl_mp['VALOR PPTO']

        porcj_mp_enl_real = ventas_mp['VALOR REAL'] / ventas_enl_mp['VALOR REAL']
        porcj_mp_enl_ppto = ventas_mp['VALOR PPTO'] / ventas_enl_mp['VALOR PPTO']

        # Dataframe de porcentajes exclusivos ACTIVACION DEMANDA
        porcentajes_act_demanda = [0.5, 0.5, 0, 0]
        df_act_demanda = calcular_valores_exclusivos(
            segmentos, porcentajes_act_demanda, sum_do
        )

        # Dataframe de porcentajes exclusivos DESARROLLO D NEGOCIO
        porcentajes_ddn_real = [porcj_enl_ddn_real, 0, porcj_el_ddn_real, porcj_do_ddn_real]
        df_ddn_real = calcular_valores_exclusivos(
            segmentos, porcentajes_ddn_real, sum_ddn
        )
        porcentajes_ddn_ppto = [porcj_enl_ddn_ppto, 0, porcj_el_ddn_ppto, porcj_do_ddn_ppto]
        df_ddn_ppto = calcular_valores_exclusivos(
            segmentos, porcentajes_ddn_ppto, sum_ddn
        )

        # Combinar ddn con ambos valores
        df_ddn = pd.merge(
            df_ddn_real[['SEGMENTO', 'VALOR REAL CALCULADO']], 
            df_ddn_ppto[['SEGMENTO', 'VALOR PPTO CALCULADO']],
            on='SEGMENTO', how='left'
        )

        # Dataframe de porcentajes exclusivos EXPERT. NO LOCALES
        porcentajes_enl_real = [porcj_enl_enl_real, porcj_mp_enl_real, 0, 0]
        df_enl_real = calcular_valores_exclusivos(
            segmentos, porcentajes_enl_real, sum_enl
        )
        porcentajes_enl_ppto = [porcj_enl_enl_ppto, porcj_mp_enl_ppto, 0, 0]
        df_enl_ppto = calcular_valores_exclusivos(
            segmentos, porcentajes_enl_ppto, sum_enl
        )

        # Combinar ddn con ambos valores
        df_enl = pd.merge(
            df_enl_real[['SEGMENTO', 'VALOR REAL CALCULADO']], 
            df_enl_ppto[['SEGMENTO', 'VALOR PPTO CALCULADO']],
            on='SEGMENTO', how='left'
        )

        # Dataframe con los valores calculados de cada ceco
        df_valores_vp_comercial = pd.concat([df_enl, df_ddn, df_act_demanda])
        df_valores_vp_comercial = (
            df_valores_vp_comercial.groupby('SEGMENTO').sum()/1000000
        ).reset_index()

        # Limpiar un poco el df
        df_valores_vp_comercial['VALOR REAL'] = df_valores_vp_comercial['VALOR REAL CALCULADO']
        df_valores_vp_comercial['VALOR PPTO'] = df_valores_vp_comercial['VALOR PPTO CALCULADO']
        df_valores_vp_comercial = df_valores_vp_comercial.drop(
            columns=['VALOR REAL CALCULADO', 'VALOR PPTO CALCULADO', 'PORCENTAJE']
        )

        # Tomar el valor de cada segmento y sacar el porcentaje con ingreso neto
        porcj_enl_real = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'EXP. NO LOCALES', 'VALOR REAL'
        ].values[0]) / ingreso_enl['VALOR REAL']
        porcj_enl_ppto = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'EXP. NO LOCALES', 'VALOR PPTO'
        ].values[0]) / ingreso_enl['VALOR PPTO']

        porcj_mp_real = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'MARCAS PROPIAS', 'VALOR REAL'
        ].values[0]) / ingreso_mp['VALOR REAL']
        porcj_mp_ppto = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'MARCAS PROPIAS', 'VALOR PPTO'
        ].values[0]) / ingreso_mp['VALOR PPTO']

        porcj_do_real = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR REAL'
        ].values[0]) / ingreso_do['VALOR REAL']
        porcj_do_ppto = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'DEMAND OWNERS', 'VALOR PPTO'
        ].values[0]) / ingreso_do['VALOR PPTO']

        porcj_el_real = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'EXP. LOCALES', 'VALOR REAL'
        ].values[0]) / ingreso_el['VALOR REAL']
        porcj_el_ppto = (df_valores_vp_comercial.loc[
            df_valores_vp_comercial['SEGMENTO'] == 'EXP. LOCALES', 'VALOR PPTO'
        ].values[0]) / ingreso_el['VALOR PPTO']

        # Dataframe de porcentajes real y ppto de VP COMERCIAL 
        porcj_exclusivo_comercial_real = {
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'],
            'PORCENTAJE': [porcj_enl_real, porcj_mp_real, porcj_el_real, porcj_do_real]
        }
        df_porcj_exclusivo_comercial_real = pd.DataFrame(porcj_exclusivo_comercial_real)

        porcj_exclusivo_comercial_ppto = {
            'SEGMENTO': ['EXP. NO LOCALES', 'MARCAS PROPIAS', 'EXP. LOCALES', 'DEMAND OWNERS'],
            'PORCENTAJE': [porcj_enl_ppto, porcj_mp_ppto, porcj_el_ppto, porcj_do_ppto]
        }
        df_porcj_exclusivo_comercial_ppto = pd.DataFrame(porcj_exclusivo_comercial_ppto)

        # Combinar los dataframe de porcentajes
        df_porcj_exclusivo_comercial_real['PORCENTAJE REAL'] = df_porcj_exclusivo_comercial_real['PORCENTAJE']
        df_porcj_exclusivo_comercial_real['PORCENTAJE PPTO'] = df_porcj_exclusivo_comercial_ppto['PORCENTAJE']
        df_porcj_exclusivo_comercial_real = df_porcj_exclusivo_comercial_real.drop(columns='PORCENTAJE')

        # Dataframe combinado para operar el porcentaje
        df_merged = pd.merge(
            df_totales_segmento, 
            df_porcj_exclusivo_comercial_real[['SEGMENTO', 'PORCENTAJE REAL', 'PORCENTAJE PPTO']],
            on='SEGMENTO', how='left'
        )
        df_merged['VALOR REAL'] = df_merged['VALOR REAL'] * df_merged['PORCENTAJE REAL']
        df_merged['VALOR PPTO'] = df_merged['VALOR PPTO'] * df_merged['PORCENTAJE PPTO']
        df_merged['DRIVER'] = 'VP COMERCIAL'

        # Eliminar filas inncesearias
        df_merged = df_merged.drop(columns=['PORCENTAJE REAL', 'PORCENTAJE PPTO'])

        # Concatenar el df combinado al df bd final
        df_bd_directos_vp_comercial= pd.concat(
            [df_merged], ignore_index=True
        )
        del df_merged

        # ========================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'FLETE FPT' * #
        # ========================================== #

        # Dataframe filtrado para Flete
        df_flete_fpt = df_edg[(df_edg['CONCEPTO'] ==  'FLETE') & (df_edg['MARCA'] !=  '')]

        # Dataframe con el total real y ppto por marca y canal
        df_flete_fpt = df_flete_fpt.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        df_flete_fpt[['VALOR REAL', 'VALOR PPTO']] = df_flete_fpt[['VALOR REAL', 'VALOR PPTO']]/1000000
        df_flete_fpt['DRIVER'] = 'FLETE'

        # Concatenar el df combinado al df bd final
        df_bd_directos_flete_fpt = pd.concat(
            [df_flete_fpt], ignore_index=True
        )

        # ================================================ #
        # * TRANSFORMACIÓN DEL RUBRO - 'FLETE COMERCIAL' * #
        # ================================================ #

        # Repartir el flete correo por canal marca
        df_vyc_canales = df_vyc[
            (df_vyc['CANAL'] != 'E-COMMERCE') &
            (df_vyc['CANAL'] != 'FABRIC PARA TERCEROS')
        ]

        # Dataframe con el total real y ppto por canal
        suma_canales = df_vyc_canales[
            df_vyc_canales['CONCEPTO'] == 'INGRESO NETO'
        ].groupby(['CANAL'])[['VALOR REAL','VALOR PPTO']].sum().reset_index()

        # Combinar los dataframe para operar
        df_canales_ing_neto = df_vyc_canales[df_vyc_canales['CONCEPTO'] == 'INGRESO NETO']
        df_canales_ing_neto_porcj = pd.merge(
            df_canales_ing_neto, suma_canales, on='CANAL', how='left'
        )

        df_canales_ing_neto_porcj['PORCENTAJE REAL'] = (
            df_canales_ing_neto_porcj['VALOR REAL_x'] / df_canales_ing_neto_porcj['VALOR REAL_y']
        )
        df_canales_ing_neto_porcj['PORCENTAJE PPTO'] = (
            df_canales_ing_neto_porcj['VALOR PPTO_x'] / df_canales_ing_neto_porcj['VALOR PPTO_y']
        )

        # Combinar con el flete para obtener el df final
        df_flete_correo[['VALOR REAL', 'VALOR PPTO']] = df_flete_correo[['VALOR REAL', 'VALOR PPTO']].fillna(0)
        df_flete_canales = pd.merge(
            df_canales_ing_neto_porcj, df_flete_correo, on='CANAL', how='left'
        )
        df_flete_canales['VALOR REAL'] = (
            df_flete_canales['PORCENTAJE REAL']*df_flete_canales['VALOR REAL']
        )/1000000
        df_flete_canales['VALOR PPTO'] = (
            df_flete_canales['PORCENTAJE PPTO']*df_flete_canales['VALOR PPTO']
        )/1000000

        # Limpiar el df combinado 
        df_flete_canales = df_flete_canales.drop(
            columns=[
                'MARCA_y', 'SEGMENTO_y',
                'CONCEPTO','AÑO', 'MES',
                'SEGMENTO_x'
            ]
        )
        df_flete_canales = df_flete_canales.rename(columns={'MARCA_x': 'MARCA'})
        df_flete_canales = df_flete_canales.groupby(
            ['CANAL', 'MARCA', 'DRIVER']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Repartir flete e-commerce
        df_flete_ecommerce = df_edg[
            (df_edg['CECO'] ==  '11515003') & 
            (df_edg['CUENTA'] ==  '"TRANSP, FLETES Y AC')
        ]

        # Dataframe con el total real y ppto por marca canal
        df_valores_flete_ecommerce = df_flete_ecommerce.groupby(
            ['MARCA', 'CANAL']
        )[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()

        # Repartir el total real y ppto por canal e-commerce
        df_vyc_ecommerce = df_vyc[df_vyc['CANAL'] == 'E-COMMERCE']
        suma_ecommerce = df_vyc_ecommerce[
            df_vyc_ecommerce['CONCEPTO'] == 'INGRESO NETO'
        ][['VALOR REAL', 'VALOR PPTO']].sum()

        df_ecommerce_ing_neto = df_vyc_ecommerce[df_vyc_ecommerce['CONCEPTO'] == 'INGRESO NETO']
        df_ecommerce_ing_neto_porcj = df_ecommerce_ing_neto.copy()

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

        # Combinar con el flete para obtener el df final
        df_merged = pd.merge(
            df_ecommerce_ing_neto_porcj, df_valores_flete_ecommerce, 
            on='CANAL', how='left'
        )
        df_merged_real = df_merged.copy()
        df_merged_real['VALOR REAL'] = (df_merged['VALOR REAL_y']*df_merged['PORCENTAJE REAL'])/1000000
        df_merged_real['VALOR PPTO'] = (df_merged['VALOR PPTO_y']*df_merged['PORCENTAJE PPTO'])/1000000
        df_merged_real['DRIVER'] = 'FLETE'
        columns_to_add = ['CANAL', 'MARCA_x', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

        # DATAFRAME FINAL DE FLETE COMERCIAL 'OK'
        df_flete_comercial = pd.concat(
            [df_merged_real[columns_to_add]], ignore_index=True
        )
        df_flete_comercial = df_flete_comercial.rename(
            columns={'MARCA_x': 'MARCA'}
        )

        # Concatenar el df combinado al df bd final
        df_bd_directos_flete = pd.concat(
            [df_flete_comercial, df_flete_canales], ignore_index=True
        )

        # =============================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'AREAS DE APOYO' * #
        # =============================================== #

        df_areas_apoyo = df_vyc[df_vyc['CONCEPTO'].isin([
            'VP RECURSOS HUMANOS', 'EMC',
            'GASTOS DE SISTEMAS', 'VP ADMINISTRATIVA',
            'PRESIDENCIA Y VP FINANCIERA'
        ])]
        total_areas_apoyo = df_areas_apoyo[['VALOR REAL', 'VALOR PPTO']].sum()

        # Dataframe de porcentajes exclusivos de los negocios 1000 y 1100
        porcj_exclusivo_areas_apoyo = {
            'NEGOCIO': ['1000', '1100'],
            'PORCENTAJE': [0.3669, 0.6331]
        }
        df_porcj_areas_apoyo = pd.DataFrame(porcj_exclusivo_areas_apoyo)

        # Operar el porcentaje
        df_porcj_areas_apoyo['AA NEGOCIO REAL'] = (
            total_areas_apoyo['VALOR REAL'] * df_porcj_areas_apoyo['PORCENTAJE']
        )
        df_porcj_areas_apoyo['AA NEGOCIO PPTO'] = (
            total_areas_apoyo['VALOR PPTO'] * df_porcj_areas_apoyo['PORCENTAJE']
        )

        # Agregar el negocio al df
        df_sum_segmento['NEGOCIO'] = df_sum_segmento['SEGMENTO'].map({
            'MARCAS PROPIAS': '1000',
            'EXP. NO LOCALES': '1000',
            'DEMAND OWNERS': '1100',
            'EXP. LOCALES': '1100'
        })
        df_sum_segmento['SUMA NEGOCIO REAL'] = df_sum_segmento.groupby('NEGOCIO')['VALOR REAL'].transform('sum')
        df_sum_segmento['SUMA NEGOCIO PPTO'] = df_sum_segmento.groupby('NEGOCIO')['VALOR PPTO'].transform('sum')

        df_sum_segmento['PORCENTAJE NEGOCIO REAL'] = (
            df_sum_segmento['VALOR REAL'] / df_sum_segmento['SUMA NEGOCIO REAL']
        )
        df_sum_segmento['PORCENTAJE NEGOCIO PPTO'] = (
            df_sum_segmento['VALOR PPTO'] / df_sum_segmento['SUMA NEGOCIO PPTO']
        )

        # Combinar los dataframe para operar 
        df_merged_porcj = pd.merge(
            df_sum_segmento, df_porcj_areas_apoyo, on='NEGOCIO', how='left'
        )
        df_merged_porcj['VALOR CALCULADO REAL'] = (
            df_merged_porcj['AA NEGOCIO REAL'] * df_merged_porcj['PORCENTAJE NEGOCIO REAL']
        )
        df_merged_porcj['VALOR CALCULADO PPTO'] = (
            df_merged_porcj['AA NEGOCIO PPTO'] * df_merged_porcj['PORCENTAJE NEGOCIO PPTO']
        )

        df_merged_porcj = df_merged_porcj.drop(
            columns=[
                'SUMA NEGOCIO REAL', 'SUMA NEGOCIO PPTO',
                'PORCENTAJE NEGOCIO REAL', 'PORCENTAJE NEGOCIO PPTO'
            ]
        )

        # Obtener el porcentaje real y ppto
        df_merged_porcj['PORCENTAJE DRIVER REAL'] = (
            df_merged_porcj['VALOR CALCULADO REAL'] / df_merged_porcj['VALOR REAL']
        )
        df_merged_porcj['PORCENTAJE DRIVER PPTO'] = (
            df_merged_porcj['VALOR CALCULADO PPTO'] / df_merged_porcj['VALOR PPTO']
        )

        # Dataframe con el valor por segmento de cada  canal y marca
        df_totales_areas = (df_vyc[df_vyc['CONCEPTO'] == 'INGRESO NETO']).groupby([
            'SEGMENTO', 'CANAL', 'MARCA'
        ])[['VALOR REAL', 'VALOR PPTO']].sum().reset_index()
        df_totales_areas['DRIVER'] = 'ÁREAS DE APOYO'

        # Dataframe combinado para operar los porcentajes y los valores
        df_merged_real = pd.merge(
            df_totales_areas, df_merged_porcj, on='SEGMENTO', how='left'
        )
        df_merged_real['VALOR FINAL REAL'] = (
            df_merged_real['VALOR REAL_x'] * df_merged_real['PORCENTAJE DRIVER REAL']
        )
        df_merged_real['VALOR FINAL PPTO'] = (
            df_merged_real['VALOR PPTO_x'] * df_merged_real['PORCENTAJE DRIVER PPTO']
        )
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR FINAL REAL', 'VALOR FINAL PPTO']

        # Concatenar el df combinado al df bd final
        df_bd_directos_areas_apoyo = pd.concat(
            [df_merged_real[columns_to_add]], 
            ignore_index=True
        )
        df_bd_directos_areas_apoyo['VALOR REAL'] = df_bd_directos_areas_apoyo['VALOR FINAL REAL']
        df_bd_directos_areas_apoyo['VALOR PPTO'] = df_bd_directos_areas_apoyo['VALOR FINAL PPTO']

        df_bd_directos_areas_apoyo = df_bd_directos_areas_apoyo.drop(
            columns=['VALOR FINAL REAL', 'VALOR FINAL PPTO']
        )

        # ==================================== #
        # * TRANSFORMACIÓN DEL RUBRO - 'PAC' * #
        # ==================================== #

        from ..cmi import pac
        df_bd_directos_pac = pac.hoja_pac(año, mes)

        columns_to_add = ['MARCA', 'CANAL', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        df_bd_directos_pac = df_bd_directos_pac[columns_to_add]

        # ========================================= #
        # * TRANSFORMACIÓN DEL RUBRO - 'REGALÍAS' * #
        # ========================================= #

        # Limpiar el df final
        df_regalias[['VALOR REAL', 'VALOR PPTO']] = df_regalias[['VALOR REAL', 'VALOR PPTO']]/1000000
        columns_to_add = ['CANAL', 'MARCA', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']

        df_bd_directos_regalias = df_regalias[columns_to_add]
        df_bd_directos_regalias

        # ============================================== #
        # * CONSOLIDAR TODOS LOS RUBROS EN UNA SOLA BD * #
        # ============================================== #

        dataframes_bd_directos = [
            df_bd_directos_trade_admon,
            df_bd_directos_inv_trade,
            df_bd_directos_otros_g_variables,
            df_bd_directos_prov_inv,
            df_bd_directos_ica,
            df_bd_directos_4X1000,
            df_bd_directos_niñas_mercadeo,
            df_bd_directos_niñas_trade,
            df_bd_directos_vp_mercadeo,
            df_bd_directos_vp_comercial,
            df_bd_directos_flete_fpt,
            df_bd_directos_flete,
            df_bd_directos_areas_apoyo,
            df_bd_directos_pac,
            df_bd_directos_regalias
        ]

        # Dataframe final de la base de datos directos
        df_bd_directos = pd.DataFrame(columns=[
            'MARCA', 'CANAL', 'DRIVER', 'VALOR REAL'
        ])
        df_bd_directos = pd.concat(dataframes_bd_directos, ignore_index=True)

        # Insertar fecha
        df_bd_directos.insert(0, 'AÑO', año)
        df_bd_directos.insert(1, 'MES', mes_letras)

        # Insertar segmentos
        df_bd_directos = utils.insert_segmentos(df_bd_directos)

        # Organizar columnas
        df_bd_directos = df_bd_directos[
            ['AÑO', 'MES', 'MARCA', 'SEGMENTO', 'CANAL', 'DRIVER', 'VALOR REAL', 'VALOR PPTO']
        ]

        # Exportar base de datos mensual
        sub_dirs_final_db = [
            'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
            'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
            'GASTOS'
        ]
        path_bd_directos = os.path.join(
            base_dir, *sub_dirs_final_db, f'BD_DIRECTOS_{mes}.{año}.xlsx'
        )
        df_bd_directos.to_excel(path_bd_directos, index=False)

        # Obtener la otra parte de los directos
        bd_g_directos = get_bd_gastos_directos(df_edg, mes, año, mes_largo)
        
        df_bd_directos = pd.concat([df_bd_directos, bd_g_directos], ignore_index=True)

        return df_bd_directos
    except Exception as e:
        import traceback
        print(f"Ha ocurrido un error: {e}")
        traceback.print_exc()
