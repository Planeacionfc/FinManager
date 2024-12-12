from model.ctsModel.ventaModel import ventaModel
from scripts.utils import utils
from scripts.cts import venta

class ventaController:
    def __init__(self):
        self.modelo = ventaModel()

    def obtener_datos_sap(self, año, mes_largo):
        var_ids = {
            'var_id_2': "Z0FISCPER_V002                0004",
        }

        var_values = {
            'var_value_ext_2': f"{mes_largo}.{año}",
        }

        data = self.modelo.get_data_venta(
            var_ids,
            var_values, 
            "ZCPA_CM10_Q0011", 
            "Z_BD_VENTA_CTS" # ! ESPERAR REUNION KEVIN
        )
        return data
    
    def transformar_datos_venta(self, data, año, mes, mes_largo):
        return venta.extract_data(data, año, mes, mes_largo)
    
if __name__ == '__main__':
    controlador = ventaController()

