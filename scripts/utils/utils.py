from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta
import os

def export_db(df: pd.DataFrame, category, año, mes):
    
    # Ruta al directorio de usuario
    base_dir = os.path.expanduser('~')
    sub_dirs_db_vyc = [
        'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
        'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
        'VENTA Y COSTO'
    ]
    sub_dirs_db_gastos = [
        'Prebel S.A BIC', 'Planeación Financiera - Documentos', 
        'AUTOMATIZACION', 'BASES DE DATOS', 'CMI', 'BD FINAL',
        'SEPARACION DE GASTOS'
    ]

    if category == 'v_y_c':
        vyc_month = os.path.join(base_dir, *sub_dirs_db_vyc, f'bd_venta_y_costo_{mes}.{año}.xlsx')
        df.to_excel(vyc_month, index=False)
        return True
    
    elif category == 'gastos':
        gastos_month = os.path.join(base_dir, *sub_dirs_db_gastos, f'bd_separacion_gastos_{mes}.{año}.xlsx')
        df.to_excel(gastos_month, index=False)
        return True
    else:
        return False


import pandas as pd
def insert_segmentos(df: pd.DataFrame):
    # Mapear y agregar la columna segmentos
    mapa_segmento = {
        'OTROS CLIENTES DO': 'DEMAND OWNERS', 
        'MARKETING PERSONAL': 'DEMAND OWNERS',
        'OMNILIFE': 'DEMAND OWNERS', 'JERONIMO MARTINS': 'DEMAND OWNERS',
        'LEONISA': 'DEMAND OWNERS', 'LOCATEL': 'DEMAND OWNERS',
        'NOVAVENTA': 'DEMAND OWNERS', 'EL ÉXITO': 'DEMAND OWNERS',
        'MILAGROS ENTERPRISE': 'DEMAND OWNERS', 'LA POPULAR': 'DEMAND OWNERS',
        'D1': 'DEMAND OWNERS', 'USA': 'DEMAND OWNERS',
        'WORMSER': 'DEMAND OWNERS',

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
        'IMPORTADOS PROCTER': 'EXP. NO LOCALES',
        'REVOX': 'EXP. NO LOCALES',
        'BURTS BEES': 'EXP. NO LOCALES',

        'ARDEN FOR MEN': 'MARCAS PROPIAS', 'NUDE': 'MARCAS PROPIAS',
        'ELIZABETH ARDEN': 'MARCAS PROPIAS', 'YARDLEY': 'MARCAS PROPIAS',
        'VITÚ': 'MARCAS PROPIAS', 'PREBEL': 'MARCAS PROPIAS',
        'OTRAS MP': 'MARCAS PROPIAS',

        'BODY CLEAR': 'NO APLICA', 'OTRAS': 'NO APLICA',
        'GILLETTE': 'NO APLICA', 'L&G ASOCIADOS': 'NO APLICA',
        'CATÁLOGO DE PRODUCTOS': 'NO APLICA', 'HINODE': 'NO APLICA',
        'PFIZER': 'NO APLICA', 'CONTEXPORT DISNEY': 'NO APLICA',
        'SYSTEM PROFESSIONAL': 'NO APLICA', 'WELONDA': 'NO APLICA',
        'SIN ASIGNAR': 'NO APLICA', 'NO APLICA': 'NO'
    }
    df['SEGMENTO'] = df['MARCA'].map(mapa_segmento)
    df = df[['CANAL','MARCA', 'SEGMENTO'] + [col for col in df.columns if col not in ['CANAL', 'MARCA', 'SEGMENTO']]]
    return df