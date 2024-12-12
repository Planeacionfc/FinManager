from flask import Blueprint, render_template, jsonify, request
from controller.ctsController import ventaController
from datetime import datetime

cts_bp = Blueprint('cts_bp', __name__)

@cts_bp.route('/home')
def home():
    return render_template('/cts/home.html')

@cts_bp.route('/api/process_cts/venta', methods=['POST'])
def process_cts_venta():

    fecha = request.json
    fecha = fecha.get('fecha')
    
    # Convertir a un objeto datetime
    fecha = datetime.strptime(fecha, "%Y-%m-%d")

    # Extraer el año y el mes
    año = fecha.year
    mes = fecha.month
    mes_largo = f"{mes:03}"
    
    # Crear un objeto tipo controlador
    controlador = ventaController.ventaController()

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
        response = controlador.transformar_datos_venta(data, año, mes, mes_largo)
        print("Todo se completo, la respuesta es: ", response)
        if response:
            return jsonify({"status": "success",
                            "message": "Proceso completado satisfactoriamente"}), 200
        else:
            return jsonify({"status": "error",
                            "message": "Ocurrio un error en la transformacion"}), 400


