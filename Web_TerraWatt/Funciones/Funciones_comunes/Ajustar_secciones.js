// Función para ajustar los márgenes superiores del encabezado y la cabecera
function ajustarEncabezado() {
    const cabecera = document.querySelector('.cabecera');
    const encabezado = document.querySelector('.encabezado');

    // Ajustar el encabezado en función de la altura de la cabecera
    if (cabecera && encabezado) {
        const alturaCabecera = cabecera.offsetHeight;
        encabezado.style.paddingTop = `${alturaCabecera}px`;
    }
}

// Ejecutar la función al cargar la página y al redimensionar la ventana
window.addEventListener('load', ajustarEncabezado);
window.addEventListener('resize', ajustarEncabezado);
