// Detecta si el idioma del documento es árabe, esto lo hacemos ya que se debe deslizar al dirección opuesta cuando la página web se encuentra en el idioma árabe 
const esArabe = document.documentElement.lang === "ar";

// Selecciona el contenedor del slider y los botones de navegación
const slider = document.querySelector('.reseñas-slider');
const btnPrev = document.getElementById('goPrevious');
const btnNext = document.getElementById('goNext');

// Índice actual del ítem visible en el slider
let currentIndex = 0;

//  botón "Siguiente"
btnNext.addEventListener('click', () => {
    const totalItems = slider.children.length; // Número total de reseñas

    if (esArabe) {
        // En árabe (RTL), al hacer clic en "siguiente", el índice disminuye
        if (currentIndex > 0) {
            currentIndex--;
            updateSliderPosition(); // Actualiza la posición visual
        }
    } else {
        // En idiomas LTR, al hacer clic en "siguiente", el índice aumenta
        if (currentIndex < totalItems - 1) {
            currentIndex++;
            updateSliderPosition(); // Actualiza la posición visual 
        }
    }
});

// botón "Anterior"
btnPrev.addEventListener('click', () => {
    const totalItems = slider.children.length;

    if (esArabe) {
        // En árabe (RTL), al hacer clic en "anterior", el índice aumenta
        if (currentIndex < totalItems - 1) {
            currentIndex++;
            updateSliderPosition(); 
        }
    } else {
        // En idiomas LTR, al hacer clic en "anterior", el índice disminuye
        if (currentIndex > 0) {
            currentIndex--;
            updateSliderPosition(); 
        }
    }
});

// Función que actualiza la posición del slider
function updateSliderPosition() {
    // Calcula el ancho de un ítem + 20 px de margen 
    const itemWidth = slider.children[0].clientWidth + 20;

    if (esArabe) {
        // En árabe, se mueve hacia la derecha (positivo)
        slider.style.transform = `translateX(${currentIndex * itemWidth}px)`;
    } else {
        // En otros idiomas, se mueve hacia la izquierda (negativo)
        slider.style.transform = `translateX(${-currentIndex * itemWidth}px)`;
    }
}
