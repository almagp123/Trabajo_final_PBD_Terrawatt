// DEBEMOS DE TENER EN CUENTA QUE PARA QUE LA FUNCION ACCESIBLE FUNCIONE DEBEMOSDE TENER INSTALADAS LAS VOCES EN NUESTRO ORDENADOR. PARA ELLO DEBEMOS DE: 
// 1. ABRIR CONFIGURACION
// 2. BUSCAR CONFIGURACION DE VOZ (DENTRO DE HORA E IDIOMA)
// 3. AGREGAR LAS VOCES NECESARIAS (NORMALMENTE EL INGLES YA VIENE PREINSTALADO POR LO QUE DEBEREMOS DE INSTALAR EL ARABE)
// 4. REINICIAR EL ORDENAR 


document.addEventListener("DOMContentLoaded", function () {
    const toggleAccesible = document.getElementById("toggleAccesible");
    toggleAccesible.addEventListener("click", function () {
        document.body.classList.toggle("modo-accesible");
    });

    window.leerTexto = function (id) {
        let texto = document.getElementById(id).textContent;
        let speech = new SpeechSynthesisUtterance();
        speech.text = texto;

        // Detecta el idioma del documento y ajusta la voz
        // Si quisieramos añadir algun idioma más deberíamos simplemente de añadir otro else if y añadir el nuevo idioma con su acronimo lang. 
        let idioma = document.documentElement.lang;
        if (idioma === "en") {
            speech.lang = "en-US"; 
        } else if (idioma === "ar") {
            speech.lang = "ar-SA"; 
        } else {
            speech.lang = "es-ES"; 
        }

        speech.rate = 1;
        speech.volume = 1;
        window.speechSynthesis.speak(speech);
    };
});

