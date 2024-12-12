from pyrfc import Connection, ABAPApplicationError, LogonError, CommunicationError

class ctsModel:
    def __init__(self):
        self.error_message = None

    def connect_sap(self):
        try:
            conexion = Connection(
            ashost="SAPBIP01",
            sysnr='00',
            client="550",
            user="DOSPINAG",
            passwd="Nov2024**", # ! OJO - CAMBIAR CUANDO EL USUARIO CAMBIE SU CONTRASEÑA
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
    

     