from pyrfc import Connection, ABAPApplicationError, LogonError, CommunicationError
class conexion:
    def __init__(self):
        self.error_message = None

    def connect(self):
        try:
            conexion = Connection(
            ashost="SAPBIP01",
            sysnr='00',
            client="550",
            user="", # USUARIO DE SAP
            passwd="", # CONTRASEÑA DE SAP
            lang="ES"
            )
        except LogonError as le:
            self.error_message = f"Credenciales incorrectas: {le}"
        except CommunicationError as ce:
            self.error_message = f"Comunicacion fallida: {ce}"
        except Exception as e:
            self.error_message = f"Error inesperado en la conexion: {e}"
        
        if self.error_message is None:
            return conexion
        else:
            return self.error_message
        
    def get_raw_data():

        try:
            # Esta lista recibe los valores que son extraidos por parseurl
            lt_parametros = [
                {
                    "NAME": "VAR_ID_2",
                    "VALUE": "Z0FISCPER_V002                0004"
                },
                
                {
                    "NAME": "VAR_VALUE_EXT_2",
                    "VALUE": "008.2024"
                }
            ]

            # El resultado es un json
            resultado = conexion.call(
                "RRW3_GET_QUERY_VIEW_DATA", # Nombre de la RFC que extrae la información de las consultas
                I_QUERY = "ZCPA_CM10_Q0011", # Nombre del query a invocar
                I_VIEW_ID = "Z_BD_VENTA_Y_COSTO_CMI", # Nombre de la vista que se crea en rsrt
                I_T_PARAMETER = lt_parametros # Tabla de parámetros definidos anteriormente
            )
            conexion.close()

            # Informacion de los ejes
            lt_axis_info = resultado['E_AXIS_INFO']
            
            # Datos en formato json de la celda del cubo (Trae los valores) 
            lt_cell_data = resultado['E_CELL_DATA']

            # Datos en formato json del eje del culo (Trae el resto de la informacion)
            lt_axis_data = resultado['E_AXIS_DATA']

            # Simbolos de texto
            lt_txt_symbols = resultado['E_TXT_SYMBOLS']

        except ABAPApplicationError as error:
            print("Error" + error.message)