function toggleExtraFields() {
    const category = document.getElementById('category').value;
    const extraFieldsVenta = document.getElementById('extrafields-venta');
    
    // Ocultar ambos conjuntos de campos adicionales
    extraFieldsVenta.style.display = 'none';
    
    // Mostrar los campos adicionales según la categoría seleccionada
    if (category === '1') {
        extraFieldsVenta.style.display = 'block';
    }
}

/* FUNCIONES DE VENTANAS MODALES*/
function confirmExecuteETL(category) {

    const fecha = document.getElementById('execution-date').value;

    if (!fecha) {
        showErrorModal("¡SELECCIONA UNA FECHA!")
        return;
    }

    // Mostrar el modal de confirmación
    document.getElementById('confirmModal').style.display = 'flex';

    // Manejar el clic en "Sí"
    document.getElementById('confirmYes').onclick = async function() {
        closeModal();

        await executeETL(category, fecha);
    };

    // Manejar el clic en "No"
    document.getElementById('confirmNo').onclick = function() {
        closeModal();
    };
}

/* FUNCION PARA EJECUTAR TODO EL PROCESO */
async function executeETL(category, fecha) {
    if(category == 'venta') {
        
        try {

            // Mostrar el modal de progreso
            document.getElementById('progressModal').style.display = 'block';
            const progressBar = document.getElementById('progressBar');
            const progressText = document.getElementById('progressText');
            let progress = 0;

            // Simular la carga de la barra de progreso
            const interval = setInterval(() => {
                if (progress < 99) {
                    progress += 1;
                    progressBar.style.width = progress + '%';
                    progressText.textContent = `Procesando... (${progress}%)`;
                }
            }, 60);

            const response = await fetch('http://172.15.1.161:8510/cts/api/process_cts/venta', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({fecha: fecha})
            });
            
            // Verificar si la respuesta fue exitosa (status code 200-299)
            if (response.ok) {

                const result = await response.json(); 
                if (result.status === 'success') {
                    document.getElementById('progressModal').style.display = 'none';
                    showSuccessModal(result.message);        
                } else {
                    document.getElementById('progressModal').style.display = 'none';
                    showErrorModal(result.message);  // Mostrar mensaje de error
                }
            } else {
                // Si el status code no es 200-299, maneja el error aquí
                document.getElementById('progressModal').style.display = 'none';
                const errorData = await response.json();  // Leer el mensaje de error
                showErrorModal(errorData.message || "Error inesperado del servidor");
            }
        } catch (error) {
            // Captura errores de red (cuando el servidor no responde)
            showErrorModal("Error de red o servidor no disponible");
        }  
    }
}


    
/* FUNCIONES DE LAS VENTANAS MODALES */
function closeModal() {
    document.getElementById('confirmModal').style.display = 'none';
}
function showErrorModal(message) {
    const errorModal = document.getElementById('errorModal'); 
    const errorMessageElement = document.getElementById('errorMessage');

    if (errorModal && errorMessageElement) {
        errorMessageElement.innerText = message; 
        errorModal.style.display = 'block';  
    } else {
        console.error('Error modal or message element not found in the DOM.');
    }
}

function closeErrorModal() {
    const errorModal = document.getElementById('errorModal');
    if (errorModal) {
        errorModal.style.display = 'none';
        resetProgress()  // Oculta el modal
    }
}

function showSuccessModal(message) {
    const successModal = document.getElementById('successModal'); 
    const successMessageElement = document.getElementById('successMessage');

    if (successModal && successMessageElement) {
        successMessageElement.innerText = message; 
        successModal.style.display = 'block';  
    } else {
        console.error('Success modal or message element not found in the DOM.');
    }
}

function closeSuccessModal() {
    const successModal = document.getElementById('successModal');
    if (successModal) {
        successModal.style.display = 'none';
        resetProgress()
          // Oculta el modal
    }
}

function resetProgress() {
    location.reload(true);
}
