from model.cmiModel.vycModel import vycModel
from scripts.utils import utils
from scripts.cmi import venta_y_costo, prov_inventario, gasto_mercadeo

class vycController:
    def __init__(self):
        self.modelo = vycModel()

    def obtener_datos_sap(self, año, mes_largo):
        var_ids = {
            'var_id_2': "Z0FISCPER_V002                0004",
        }

        var_values = {
            'var_value_ext_2': f"{mes_largo}.{año}",
        }

        data = self.modelo.get_data_vyc(
            var_ids,
            var_values, 
            "ZCPA_CM10_Q0011", 
            "Z_BD_VENTA_Y_COSTO_CMI"
        )
        return data
    
    def transformar_datos_vyc(self, data, año, mes, mes_largo):
        return venta_y_costo.extract_data(data, año, mes, mes_largo)
    
    def transformar_datos_prov_inventario(self, data):
        return prov_inventario.extract_data(data)
    
    def transformar_datos_gasto_mercadeo(self, data):
        return gasto_mercadeo.extract_data(data)

if __name__ == '__main__':
    controlador = vycController()

