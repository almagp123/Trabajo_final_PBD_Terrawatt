// Esperamos a q se carge toda la página web para poder ejecutar este código
document.addEventListener("DOMContentLoaded", function () {

    // Obtenemos la información de nuestro documento Boton_subir.html
    fetch("boton_subir.html")
        .then(response => response.text())  
        .then(data => {

            // Se inserta el el html donde se encuentroel id de boton-placeholder, por otro lado identificamos el elemento con su id, en este caso "backToTop", por otro lado definimos que el elemento solo se muestre si deslizas mas de 300 px hacia abajo, y si no no se muestre, porque se entiende que ya estas al prinicpio de la página 
            document.getElementById("boton-placeholder").innerHTML = data;
            const backToTopButton = document.getElementById("backToTop");
            window.addEventListener("scroll", function () {
                if (window.scrollY > 300) {
                    backToTopButton.classList.add("show");
                } else {
                    backToTopButton.classList.remove("show");
                }
            });

            // Con esta funcion detectamos cuando se hace click en este elemento, y se establece que debe de volver a la posicion 0, arriba y con una animacion fluida 
            backToTopButton.addEventListener("click", function () {
                window.scrollTo({
                    top: 0,              
                    behavior: "smooth"    
                });
            });
        });
});
