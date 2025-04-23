function inicializarSelectorIdioma() {

    const idiomaBoton = document.querySelector('.idioma-boton');
    const idiomaMenu = document.querySelector('.idioma-menu');

    if (idiomaBoton && idiomaMenu) {
        idiomaBoton.addEventListener('click', (event) => {
            event.stopPropagation(); // Evita que la opciones aparezcan solo cuando tienes el raton por encima si no que se quede activo hasta que sepinche fuera
            idiomaMenu.classList.toggle('activo'); 
        });

        // Cierra el menú cuando se hace clic fuera
        document.addEventListener('click', (event) => {
            if (!idiomaBoton.contains(event.target) && !idiomaMenu.contains(event.target)) {
                idiomaMenu.classList.remove('activo');
            }
        });
    } else {
        console.warn('No se encontraron el botón de idioma o el menú de idioma.');
    }
}

// Cargar la barra de navegación y activar la hamburguesa e idioma después de cargar
cargarComponente('Barra_navegacion.html', 'header-placeholder', () => {
    // Realmente no haría falta inicializar la hamburguesa otra vez ya que se inicializa cuando se carga la barra de navegacion.
    inicializarHamburguesa();
    inicializarSelectorIdioma();
});



