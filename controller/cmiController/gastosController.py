from model.cmiModel.gastosModel import gastosModel
from scripts.utils import utils
from scripts.cmi import gastos

class gastosController:
    def __init__(self):
        self.modelo = gastosModel()

    def obtener_datos_sap(self, año):
       
        var_ids = {
            'var_id_2': "0P_FYRA                       0004",
        }

        var_values = {
            'var_value_ext_2': f"{año}",
        }

        data = self.modelo.get_data_gastos(
            var_ids,
            var_values,
            "0CCA_MP01_Q001", 
            "Z_BD_GASTOS"
        )
        return data
    
    def transformar_datos(self, data, año, mes, mes_largo):
        return gastos.extract_data(data, año, mes, mes_largo)

if __name__ == '__main__':
    controlador = gastosController()

