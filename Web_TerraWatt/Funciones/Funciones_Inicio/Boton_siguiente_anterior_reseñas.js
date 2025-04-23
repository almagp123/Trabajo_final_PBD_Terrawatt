// PREGUNTAR TEMA DE LAS RESEÑAS EN ARABE !!!!!!!!!! 



// // Definimos el contenedor en el que estan las reseñas y los ID de los botones que hemos definido.
// const slider = document.querySelector('.reseñas-slider');
// const btnPrev = document.getElementById('goPrevious');
// const btnNext = document.getElementById('goNext');

// // Definimos la variable en 0
// let currentIndex = 0;

// // Definimos que pasasi le damos al boton con el id de next (siguiente), en el cual debemosprimero de obtener el número total de reseñas, debemos de primero verificar si estamos en la última reseña, ya que si estamos enla ultima nopodremos avanzar mas, por lo que si no estamosen la última reseña se incrementara 1 el indice y se actualiza la posicion de la reseña principal con una funcion definida al final del script

// btnNext.addEventListener('click', () => {
//     const totalItems = slider.children.length;
//     if (currentIndex < totalItems - 1) {
//         currentIndex++; 
//         updateSliderPosition(); 
//     }
// });

// // Hacemos esencialmente lo mismo que en elemento anterior, solo que esta vez comprovamos que no estamos en la primera reseña ya que si estamos en la primera reseña no podremos volver a la enterior, luego ssi no se esta en la primera reseña se disminuye 1 el indice y se actualiza la posicion de la slider (igual que la enterior, con la funcion que se encuentra al final del script)

// btnPrev.addEventListener('click', () => {
//     if (currentIndex > 0) {
//         currentIndex--; 
//         updateSliderPosition(); 
//     }
// });

// // Función para actualizar la posición del slider
// // Definimos la funcion que actualiza la posicion del slider, en este caso, calculamos el ancho de lareseña y contando que debe de tener un margen entre ellas de 20 pixeles, luego las movemos horizontalmente con CSS tranform, y aplicamos la fórmula dependiendo del indice que se haya elegido y los pixeles de cada reseña, para saber cuantos pixeles debe de avanzar o retroceder. 

// function updateSliderPosition() {
//     const itemWidth = slider.children[0].clientWidth + 20;
//     slider.style.transform = `translateX(${-currentIndex * itemWidth}px)`;
// }







const esArabe = document.documentElement.lang === "ar";

const slider = document.querySelector('.reseñas-slider');
const btnPrev = document.getElementById('goPrevious');
const btnNext = document.getElementById('goNext');

let currentIndex = 0;

btnNext.addEventListener('click', () => {
    const totalItems = slider.children.length;
    
    if (esArabe) { 
        if (currentIndex > 0) {
            currentIndex--; 
            updateSliderPosition(); 
        }
    } else { 
        if (currentIndex < totalItems - 1) {
            currentIndex++; 
            updateSliderPosition(); 
        }
    }
});

btnPrev.addEventListener('click', () => {
    const totalItems = slider.children.length;

    if (esArabe) { 
        if (currentIndex < totalItems - 1) {
            currentIndex++; 
            updateSliderPosition(); 
        }
    } else { 
        if (currentIndex > 0) {
            currentIndex--; 
            updateSliderPosition(); 
        }
    }
});

function updateSliderPosition() {
    const itemWidth = slider.children[0].clientWidth + 20; 

    if (esArabe) {
        slider.style.transform = `translateX(${currentIndex * itemWidth}px)`; 
    } else {
        slider.style.transform = `translateX(${-currentIndex * itemWidth}px)`; 
    }
}



