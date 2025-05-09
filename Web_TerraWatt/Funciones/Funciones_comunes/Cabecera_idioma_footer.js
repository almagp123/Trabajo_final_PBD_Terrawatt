// (() => {
//     // Función para cargar un componente HTML en un contenedor específico. Además, ejecuta un callback opcional una vez cargado, útil para inicializar comportamientos como el menú hamburguesa.
//     function cargarComponente(url, contenedorId, callback) {
//         fetch(url)
//             .then(response => {
//                 if (!response.ok) throw new Error(`Error al cargar ${url}: ${response.status}`);
//                 return response.text();
//             })
//             .then(data => {
//                 document.getElementById(contenedorId).innerHTML = data;
//                 console.log(`${url} cargado correctamente.`);
//                 if (callback) callback();
//             })
//             .catch(error => console.error(`Error al cargar ${url}:`, error));
//     }

    
//     // Obtenemos el idioma del documento
//     let idioma = document.documentElement.lang;

//     // Determinamos las rutas de navegación y pie de página según el idioma
//     let rutaNavegacion, rutaPiePagina;

//     if (idioma === "es") {
//         rutaNavegacion = "./Barra_navegacion.html";
//         rutaPiePagina = "./Pie_de_pagina.html";
//     } else if (idioma === "en") {
//         rutaNavegacion = "../en/Barra_navegacion.html";
//         rutaPiePagina = "../en/Pie_de_pagina.html";
//     } else if (idioma === "ar") {
//         rutaNavegacion = "../ar/Barra_navegacion.html";
//         rutaPiePagina = "../ar/Pie_de_pagina.html";
//     } else {
//         console.warn(`Idioma no reconocido: ${idioma}. Se usará la versión en español por defecto.`);
//         rutaNavegacion = "./Barra_navegacion.html";
//         rutaPiePagina = "./Pie_de_pagina.html";
//     }

//     // Función para inicializar la lógica del botón hamburguesa en pantallas pequeñas
//     function inicializarHamburguesa() {
//         const hamburguesa = document.querySelector('.hamburguesa');
//         const barraNav = document.querySelector('.barra-navegacion');

//         if (hamburguesa && barraNav) {
//             hamburguesa.addEventListener('click', () => {
//                 hamburguesa.classList.toggle('active');
//                 barraNav.classList.toggle('active');
//                 console.log("Menú hamburguesa activado/desactivado.");
//             });

//             document.addEventListener('click', (event) => {
//                 if (!hamburguesa.contains(event.target) && !barraNav.contains(event.target)) {
//                     hamburguesa.classList.remove('active');
//                     barraNav.classList.remove('active');
//                     console.log("Menú hamburguesa cerrado por clic externo.");
//                 }
//             });
//         } else {
//             console.warn('No se encontró el botón hamburguesa o la barra de navegación.');
//         }
//     }

//     // Cargar componentes
//     cargarComponente(rutaNavegacion, 'header-placeholder', inicializarHamburguesa);
//     cargarComponente(rutaPiePagina, 'footer-placeholder');
// })();




(() => {
    function cargarComponente(url, contenedorId, callback) {
        fetch(url)
            .then(response => {
                if (!response.ok) throw new Error(`Error al cargar ${url}: ${response.status}`);
                return response.text();
            })
            .then(data => {
                document.getElementById(contenedorId).innerHTML = data;
                if (callback) callback();
            })
            .catch(error => console.error(`Error al cargar ${url}:`, error));
    }

    let idioma = document.documentElement.lang;
    let rutaNavegacion, rutaPiePagina;

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
        console.warn(`Idioma no reconocido: ${idioma}. Se usará la versión en español por defecto.`);
        rutaNavegacion = "./Barra_navegacion.html";
        rutaPiePagina = "./Pie_de_pagina.html";
    }

    function inicializarHamburguesa() {
        const hamburguesa = document.querySelector('.hamburguesa');
        const barraNav = document.querySelector('.barra-navegacion');

        if (hamburguesa && barraNav) {
            hamburguesa.addEventListener('click', () => {
                hamburguesa.classList.toggle('active');
                barraNav.classList.toggle('active');
            });

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

    function inicializarSelectorIdioma() {
        const idiomaBoton = document.querySelector('.idioma-boton');
        const idiomaMenu = document.querySelector('.idioma-menu');

        if (idiomaBoton && idiomaMenu) {
            idiomaBoton.addEventListener('click', (event) => {
                event.stopPropagation();
                idiomaMenu.classList.toggle('activo');
            });

            document.addEventListener('click', (event) => {
                if (!idiomaBoton.contains(event.target) && !idiomaMenu.contains(event.target)) {
                    idiomaMenu.classList.remove('activo');
                }
            });
        } else {
            console.warn('No se encontraron el botón de idioma o el menú de idioma.');
        }
    }

    // ✅ TODAS las llamadas deben estar dentro del IIFE
    cargarComponente(rutaNavegacion, 'header-placeholder', () => {
        inicializarHamburguesa();
        inicializarSelectorIdioma();
    });

    cargarComponente(rutaPiePagina, 'footer-placeholder');
})();

