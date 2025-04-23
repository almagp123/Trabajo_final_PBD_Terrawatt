// Función para cargar un componente HTML en un contenedor específico especófico, además con esta ejecutamos un callback para poder ejecutar la función de hamburguesa una ves que este cargada la barra de navgeación y sea necesario, es decir si la pantalla es más pequeña y se convierte de barra de navegacion a hmaburguesa
function cargarComponente(url, contenedorId, callback) {
    fetch(url)
        .then(response => {
            if (!response.ok) throw new Error(`Error al cargar ${url}: ${response.status}`);
            return response.text();
        })
        .then(data => {
            document.getElementById(contenedorId).innerHTML = data;
            console.log(` ${url} cargado exitosamente.`);
            if (callback) callback(); r
        })
        .catch(error => console.error(`Error al cargar ${url}:`, error));
}

// Definimos la contante del idioma, con el lang del documento
const idioma = document.documentElement.lang;

// Definimos las rutas ya que dependienod del idioma de la página se encuentra en una carpeta distinta. 
let rutaNavegacion, rutaPiePagina;

// Rutas dependiendo del idioma, además establecemos que si no se detecta el idioma se ponga prederminadamente la de español, para que el elemento no quede vacio.
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
    console.warn(`⚠️ Idioma no reconocido: ${idioma}. Se usará la versión en español por defecto.`);
    rutaNavegacion = "./Barra_navegacion.html";
    rutaPiePagina = "./Pie_de_pagina.html";
}

//Función para activar el botón hamburguesa, imprimos por consola algunos avisos para poder comporbar que su funcionamientoes el adecuado
function inicializarHamburguesa() {
    console.log("Lógica de la hamburguesa inicializada.");

    const hamburguesa = document.querySelector('.hamburguesa');
    const barraNav = document.querySelector('.barra-navegacion');

    if (hamburguesa && barraNav) {
        hamburguesa.addEventListener('click', () => {
            hamburguesa.classList.toggle('active');
            barraNav.classList.toggle('active');
            console.log("Menú hamburguesa activado/desactivado.");
        });

        document.addEventListener('click', (event) => {
            if (!hamburguesa.contains(event.target) && !barraNav.contains(event.target)) {
                hamburguesa.classList.remove('active');
                barraNav.classList.remove('active');
                console.log("Menú hamburguesa cerrado por clic externo.");
            }
        });
    } else {
        console.warn('No se encontraron el botón de hamburguesa o la barra de navegación.');
    }
}

// Cargar la barra de navegación y activar la hamburguesa después de cargar, si es necesario
cargarComponente(rutaNavegacion, 'header-placeholder', inicializarHamburguesa);

// Cargamos el pie de página. 
cargarComponente(rutaPiePagina, 'footer-placeholder');

