from flask import Blueprint, render_template, jsonify, request
from controller.cmiController import vycController
from controller.cmiController import costosPerController, gastosController
from datetime import datetime

cmi_bp = Blueprint('cmi_bp', __name__)

@cmi_bp.route('/api/process_cmi/v_y_c', methods=['POST'])
def process_cmi_vyc():

    fecha = request.json
    fecha = fecha.get('fecha')
    
    # Convertir a un objeto datetime
    fecha = datetime.strptime(fecha, "%Y-%m-%d")

    # Extraer el año y el mes
    año = fecha.year
    mes = fecha.month
    mes_largo = f"{mes:03}"
    
    # Crear un objeto tipo controlador
    controlador = vycController.vycController()

    # Obtener los datos desde el controlador
    data = controlador.obtener_datos_sap(año, mes_largo)

    # Verificar que hayan llegado correctamente
    if "error" in data:
        error_message = data["error"]
        
        if "RFC_LOGON_FAILURE" in error_message:
            credentials_message = "Credenciales de SAP incorrectas."
            return jsonify({"status": "error",
                                "message": credentials_message}), 400
        
        return jsonify({"status": "error",
                            "message": error_message}), 400
    else:
        # Si la data llego correctamente, transformarla
        response = controlador.transformar_datos_vyc(data, año, mes, mes_largo)
        print("Todo se completo, la respuesta es: ", response)
        if response:
            return jsonify({"status": "success",
                            "message": "Proceso completado satisfactoriamente"}), 200
        else:
            return jsonify({"status": "error",
                            "message": "Ocurrio un error en la transformacion"}), 400
        
@cmi_bp.route('/api/process_cmi/gastos', methods=['POST'])
def process_cmi_gastos():

    fecha = request.json
    fecha = fecha.get('fecha')
    
    # Convertir a un objeto datetime
    fecha = datetime.strptime(fecha, "%Y-%m-%d")

    # Extraer el año y el mes
    año = fecha.year
    mes = fecha.month
    mes_largo = f"{mes:03}"

    # Crear un objeto tipo controlador
    controlador = gastosController.gastosController()

    # Obtener los datos desde el controlador
    data = controlador.obtener_datos_sap(año)

    # Verificar que hayan llegado correctamente
    if "error" in data:
        error_message = data["error"]
        
        if "RFC_LOGON_FAILURE" in error_message:
            credentials_message = "Credenciales de SAP incorrectas."
            return jsonify({"status": "error",
                                "message": credentials_message}), 400
        
        return jsonify({"status": "error",
                            "message": error_message}), 400
    else:
        # Si la data llego correctamente, transformarla
        response = controlador.transformar_datos(data, año, mes, mes_largo)
        print("Todo se completo, la respuesta es: ", response)
        if response:
            return jsonify({"status": "success",
                            "message": "Proceso completado satisfactoriamente"}), 200
        else:
            return jsonify({"status": "error",
                            "message": "Ocurrio un error en la transformacion"}), 400
        
        
def process_cmi_costos_per(año, mes_largo):
    
    # Crear un objeto tipo controlador
    controlador = costosPerController.costosPerController()

    # Obtener los datos desde el controlador
    data = controlador.obtener_datos_sap(año, mes_largo)

    # Verificar que hayan llegado correctamente
    if "error" in data:
        error_message = data["error"]
        
        if "RFC_LOGON_FAILURE" in error_message:
            credentials_message = "Credenciales de SAP incorrectas."
            return credentials_message
        
        return error_message
    else:
        # Si la data llego correctamente, transformarla
        response = controlador.transformar_datos_costos_per(data)
        return response # Para este caso es un df

@cmi_bp.route('/home')
def home():
    return render_template('/cmi/home.html')

@cmi_bp.route('/etl')
def transform():
    return render_template('/cmi/transform.html')


