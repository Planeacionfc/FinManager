from model.cmiModel.costosPerModel import costosPerModel
from scripts.utils import utils
from scripts.cmi import costos_per
class costosPerController:
    def __init__(self):
        self.modelo = costosPerModel()

    def obtener_datos_sap(self, año, mes_largo):
        var_ids = {
            'var_id_2': "Z0FISCPER_V002                0004",
        }

        var_values = {
            'var_value_ext_2': f"{mes_largo}.{año}",
        }

        data = self.modelo.get_data_costos_per(
            var_ids,
            var_values, 
            "ZCPA_CM10_Q0011", 
            "Z_CMI_VENTA_Y_COSTO"
        )
        return data
    
    def transformar_datos_costos_per(self, data):
        return costos_per.extract_data(data)

if __name__ == '__main__':
    controlador = costosPerController()

