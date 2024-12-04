from pyrfc import Connection, ABAPApplicationError, LogonError, CommunicationError

class vycModel:
    def __init__(self):
        self.error_message = None

    def connect_sap(self):
        try:
            conexion = Connection(
            ashost="SAPBIP01",
            sysnr='00',
            client="550",
            user="DOSPINAG",
            passwd="Nov2024**",
            lang="ES"
            )
        except LogonError as le:
            self.error_message = f"Credenciales incorrectas: {le}"
        except CommunicationError:
            self.error_message = f"Comunicacion fallida: Intente conectarse a la red de Prebel"
        except Exception as e:
            self.error_message = f"Error inesperado en la conexion: {e}"
        
        if self.error_message is None:
            return conexion
        else:
            return self.error_message
    
    def get_data_vyc(self, var_ids, var_values, query, view_id):
        
        # Conectarse a SAP
        conexion = vycModel.connect_sap(self)

        if self.error_message != None:
            return {"error": self.error_message}
        else:
            try:
                lt_parametros = [
                    {
                        "NAME": "VAR_ID_2", "VALUE": var_ids['var_id_2']
                    },
                    {
                        "NAME": "VAR_VALUE_EXT_2", "VALUE": var_values['var_value_ext_2']
                    }
                ]
                resultado = conexion.call(
                    "RRW3_GET_QUERY_VIEW_DATA",
                    I_QUERY=query, 
                    I_VIEW_ID=view_id, 
                    I_T_PARAMETER=lt_parametros
                )
                conexion.close()
                return resultado

            except ABAPApplicationError as error:
                return {"error": error.message}
            except Exception as e:
                return {"error": f"Error inesperado: {e}"}