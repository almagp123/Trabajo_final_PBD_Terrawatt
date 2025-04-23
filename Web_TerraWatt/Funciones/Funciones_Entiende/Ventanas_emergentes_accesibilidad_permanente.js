// Esta funcion es un poco más compleja de las demás ya que esta genera el modal, separa e identifica los textos, que son titulos, informacion o sub informacion, además por otro lado define el modal que nos permite cerrar la pantalla

document.addEventListener("DOMContentLoaded", () => {
    // Definimos todas las contantes que vamos a necisitar/modificar
    const modal = document.getElementById("modal");
    const modalTitle = document.getElementById("modal-title");
    const modalInfo = document.getElementById("modal-info");
    const modalSubinfo = document.getElementById("modal-subinfo");
    const closeBtn = document.querySelector(".close");
    const sections = document.querySelectorAll(".factura-section");

    let lastFocusedElement; 

    function openModal(section) {
        lastFocusedElement = document.activeElement; 

        // Obtiene los datos para proporcionarselos al modal.
        const titleText = section.getAttribute("data-title") || "Sin título";
        const mainText = section.getAttribute("data-info") || "Sin contenido disponible.";
        const subinfoText = section.getAttribute("data-subinfo") || "";

        // Establecemos que tl titulo es un h2, para que se le añada el estilo
        modalTitle.innerHTML = `<h2>${titleText}</h2>`;

        // Em este caso estamos dandole el formato a los párrafor de nuetsro html, como por ejemplo estableciendo saltos de línea cuando hay una lista de elementos o poniendo en negrita los títulos de explicaciones como puede ser este ejemplo: 
        // Contrato de mercado: El tipo de contrato de electricidad (regulado o libre) y el nombre de la tarifa asociada.
        // En este caso reconoce que lo que hay antes de los dos puntos es el título"" de lo que vamos a explicar acontinuacion y lo reconoce en negrita. 
        modalInfo.innerHTML = "";
        mainText.split("\n").forEach(paragraph => {
            if (paragraph.trim() !== "") {
                const p = document.createElement("p");
                if (paragraph.includes(":")) {
                    const [boldText, normalText] = paragraph.split(":");
                    const b = document.createElement("b");
                    b.textContent = boldText.trim() + ":";
                    p.appendChild(b);
                    p.append(" " + normalText.trim());
                } else {
                    p.textContent = paragraph.trim();
                }
                modalInfo.appendChild(p);
            }
        });

        // Creamos una lista si hay subinformación
        modalSubinfo.innerHTML = "";
        if (subinfoText) {
            const subinfoItems = subinfoText.split(",");
            const ul = document.createElement("ul");
            subinfoItems.forEach(item => {
                const li = document.createElement("li");
                li.textContent = item.trim();
                ul.appendChild(li);
            });
            modalSubinfo.appendChild(ul);
        }

        // // Eliminamos el boton de accesibilidad si existe (y su informacion), luego se autogenera otro, esto es implemente para evitar que el modal que se abra se quede con la informacion del modal anterior evitando que se lea siempre el texto del modal que primero se abra. 
        let btnLeerExistente = document.getElementById("btn-leer-modal");
        if (btnLeerExistente) {
            btnLeerExistente.remove();
        }

        // Creamos y definimos el boton de accesibilidad
        let btnLeer = document.createElement("button");
        btnLeer.classList.add("btn-accesibilidad");
        btnLeer.id = "btn-leer-modal";
        btnLeer.innerHTML = '<i class="fas fa-universal-access"></i>';

        // Usamos la fucion de leerTexto que esta explicado más adelante para que lea nuestras tres contantes, el titulo, el texto principal y la subinformaicon. 
        btnLeer.addEventListener("click", function () {
            leerTexto(titleText, mainText, subinfoText);
        });

        modal.querySelector(".modal-content").appendChild(btnLeer);

        // Mostramos el modal y mover foco al botón de cierre, ya que el primer foton al que podemos acceder
        modal.style.display = "flex";
        modal.setAttribute("aria-hidden", "false");
        closeBtn.focus();
    }




    ///////
    function closeModal() {
        modal.style.display = "none";
        modal.setAttribute("aria-hidden", "true");

        // Devolver foco al elemento que abrió el modal
        if (lastFocusedElement) {
            lastFocusedElement.focus();
        }

        // Detener la lectura si se cierra la ventana emergente 
        window.speechSynthesis.cancel();
    }

    // Esta es la funcion que nos permite poder leer el texto de dentro del modal, es esencialmente la misma que hemos usado en la funcion de Idioma.js, que s eencuentra en funciones comunes, ya que es la que se usa para leer el resto de los textos. 
    function leerTexto(title, info, subinfo) {
        let texto = `${title}. ${info}. ${subinfo}`;

        window.speechSynthesis.cancel();

        let speech = new SpeechSynthesisUtterance();
        speech.text = texto;

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

        setTimeout(() => {
            window.speechSynthesis.speak(speech);
        }, 200);
    }

    // Esto nos permite que todo sea navegable con el teclado, como por ejemplo con el enter, tab o el espacio
    sections.forEach(section => {
        section.setAttribute("tabindex", "0");
        section.setAttribute("role", "button");

        section.addEventListener("click", () => openModal(section));
        section.addEventListener("keydown", (event) => {
            if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                openModal(section);
            }
        });
    });

    //  Se cierra el modal con un click
    closeBtn.addEventListener("click", closeModal);

    // Se cierra el modal con el teclado ya sea con el enter o con un espacio 
    closeBtn.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
            closeModal();
        }
    });

    window.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            closeModal();
        }
    });

    // Se cierra el modal con un click fuera de la ventana emergente 
    window.addEventListener("click", (e) => {
        if (e.target === modal) {
            closeModal();
        }
    });
});
