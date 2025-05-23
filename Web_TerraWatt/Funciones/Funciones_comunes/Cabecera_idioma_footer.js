
(() => {


    function cargarComponente(url, contenedorId, callback) {
        fetch(url) // Hace una petición HTTP al archivo HTML
            .then(response => {
                if (!response.ok) throw new Error(`Error al cargar ${url}: ${response.status}`);
                return response.text(); 
            })
            .then(data => {
                // Inserta el contenido HTML en el contenedor especificado
                document.getElementById(contenedorId).innerHTML = data;
                if (callback) callback();
            })
            .catch(error => console.error(`Error al cargar ${url}:`, error));
    }

    // Obtiene el idioma actual de la página desde el atributo lang del elemento <html>
    let idioma = document.documentElement.lang;
    let rutaNavegacion, rutaPiePagina;

    // Asigna las rutas de navegación y pie de página según el idioma
    if (idioma === "es") {
        rutaNavegacion = "./Barra_navegacion.html";
        rutaPiePagina = "./Pie_de_pagina.html";
    } else if (idioma === "en") {
        rutaNavegacion = "../en/Barra_navegacion.html";
        rutaPiePagina = "../en/Pie_de_pagina.html";
    } else if (idioma === "ar") {
        rutaNavegacion = "../ar/Barra_navegacion.html";
        rutaPiePagina = "../ar/Pie_de_pagina.html";
    } else {
        // Idioma no reconocido: se usa español como valor por defecto
        console.warn(`Idioma no reconocido: ${idioma}. Se usará la versión en español por defecto.`);
        rutaNavegacion = "./Barra_navegacion.html";
        rutaPiePagina = "./Pie_de_pagina.html";
    }

    // Función para inicializar el botón hamburguesa
    function inicializarHamburguesa() {
        const hamburguesa = document.querySelector('.hamburguesa');
        const barraNav = document.querySelector('.barra-navegacion');

        if (hamburguesa && barraNav) {
            // Toggle para mostrar u ocultar el menú al hacer clic en el icono
            hamburguesa.addEventListener('click', () => {
                hamburguesa.classList.toggle('active');
                barraNav.classList.toggle('active');
            });

            // Cierra el menú si se hace clic fuera del mismo
            document.addEventListener('click', (event) => {
                if (!hamburguesa.contains(event.target) && !barraNav.contains(event.target)) {
                    hamburguesa.classList.remove('active');
                    barraNav.classList.remove('active');
                }
            });
        } else {
            console.warn('No se encontró el botón hamburguesa o la barra de navegación.');
        }
    }

    // Función para inicializar el selector de idioma 
    function inicializarSelectorIdioma() {
        const idiomaBoton = document.querySelector('.idioma-boton');
        const idiomaMenu = document.querySelector('.idioma-menu');

        if (idiomaBoton && idiomaMenu) {
            // Toggle del menú de idioma al hacer clic
            idiomaBoton.addEventListener('click', (event) => {
                event.stopPropagation(); 
                idiomaMenu.classList.toggle('activo');
            });

            // Oculta el menú si se hace clic fuera del botón o del menú
            document.addEventListener('click', (event) => {
                if (!idiomaBoton.contains(event.target) && !idiomaMenu.contains(event.target)) {
                    idiomaMenu.classList.remove('activo');
                }
            });
        } else {
            console.warn('No se encontraron el botón de idioma o el menú de idioma.');
        }
    }

    // Carga la barra de navegación en el contenedor con ID 'header-placeholder'
    // Después de cargarla, se inicializa el botón hamburguesa y el selector de idioma
    cargarComponente(rutaNavegacion, 'header-placeholder', () => {
        inicializarHamburguesa();
        inicializarSelectorIdioma();
    });

    // Carga el pie de página en el contenedor con ID 'footer-placeholder'
    cargarComponente(rutaPiePagina, 'footer-placeholder');

})();
