// NO TENER ENCUENTA SEGURAMENTE HAYA QUE CAMBIARLO TODO CUANDO SE CONECTE AL DOCKER YA QUE YA NO SE PODRÁ USAR UNA FAST API
// NO BORRAR PARA TENER DE REFERENCIA EL CÓDIGO!!!!


// Función para manejar botones activos
function activarBoton(botonSeleccionado, botones) {
  botones.forEach(boton => boton.classList.remove("activo"));
  botonSeleccionado.classList.add("activo");
}

// Botones principales y su funcionalidad
const botonesOpciones = document.querySelectorAll('.boton');
const seccionDesplegableBase = document.getElementById("seccion-desplegable-base");
const avisoTrabajando = document.getElementById("aviso-trabajando");

botonesOpciones.forEach(boton => {
  boton.addEventListener('click', () => {
    if (boton.id === "opcion-base") {
      activarBoton(boton, botonesOpciones);
      seccionDesplegableBase.classList.add("activo");
      avisoTrabajando.classList.remove("activo");
    } else if (boton.id === "opcion-valle") {
      activarBoton(boton, botonesOpciones);
      seccionDesplegableBase.classList.remove("activo");
      avisoTrabajando.classList.add("activo");
    }
  });
});

// Función genérica para manejar incrementos y decrementos
function manejarIncrementoDecremento(decrementBtn, incrementBtn, input, step = 1) {
  decrementBtn.addEventListener("click", () => {
    const currentValue = parseFloat(input.value);
    const minValue = parseFloat(input.min);
    if (currentValue > minValue) {
      input.value = (currentValue - step).toFixed(1);
    }
  });

  incrementBtn.addEventListener("click", () => {
    const currentValue = parseFloat(input.value);
    const maxValue = parseFloat(input.max);
    if (currentValue < maxValue) {
      input.value = (currentValue + step).toFixed(1);
    }
  });
}

// Manejo de los controles de incremento/decremento
manejarIncrementoDecremento(
  document.getElementById("decrement"),
  document.getElementById("increment"),
  document.getElementById("potencia"),
  0.5
);

manejarIncrementoDecremento(
  document.getElementById("decrement-residentes"),
  document.getElementById("increment-residentes"),
  document.getElementById("numero_residentes"),
  1
);




function enviarDatos() {
  // Capturar los valores del formulario
  const potencia = parseFloat(document.getElementById("potencia").value);
  console.log("Potencia capturada como número:", potencia);

  const numero_residentes = parseInt(document.getElementById("numero_residentes").value, 10);
  const tipo_vivienda = document.getElementById("tipo_vivienda").value;
  const provincia = document.getElementById("provincia").value;
  const mes = parseInt(document.getElementById("mes").value);

  // Construir el objeto de datos a enviar
  const datos = {
    potencia: potencia,
    numero_residentes: numero_residentes,
    tipo_vivienda: tipo_vivienda,
    provincia: provincia,
    mes: mes
  };
  console.log("Datos enviados al backend:", datos);

  // Realizar la petición fetch
  fetch("http://127.0.0.1:8000/transformar", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(datos),
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Respuesta recibida del backend:", data);

      const resultadoDiv = document.getElementById("resultado");
      const transformados = data.datos_transformados;

      if (!transformados) {
        console.error("Error: No se encontraron datos transformados en la respuesta del backend.");
        resultadoDiv.innerHTML = "<p style='color: red;'>Error al obtener datos transformados.</p>";
        return;
      }
      console.log("Datos transformados:", transformados);

      // Procesar la predicción de consumo
      const consumo = transformados.prediccion_consumo;
      const consumoFormateado = consumo.toFixed(2) + " kWh";
      
      // Procesar el precio medio (convertir €/MWh a €/kWh)
      let precioMedioKWh = 0;
      if (transformados.precio && typeof transformados.precio === "object") {
        precioMedioKWh = (transformados.precio.precio_medio / 1000).toFixed(4); // €/kWh
      }

      // Calcular el costo fijo de la potencia contratada usando el precio medio
      const dias = 30; // Supongamos 30 días para el cálculo
      const costoPotencia = potencia * precioMedioKWh * dias; // €/kW/día

      // Calcular el costo total estimado de la factura
      const costoEnergia = consumo * precioMedioKWh; // €/kWh * kWh
      const costoTotalFactura = parseFloat(costoEnergia) + parseFloat(costoPotencia);

      // Mostrar los resultados en HTML
      let resultadosHTML = `<p><strong>Consumo predicho:</strong> ${consumoFormateado}</p>`;
      resultadosHTML += `<p><strong>Precio medio:</strong> ${precioMedioKWh} €/kWh</p>`;
      resultadosHTML += `<p><strong>Coste potencia contratada:</strong> ${costoPotencia.toFixed(2)} €</p>`;
      resultadosHTML += `<p><strong>Costo total estimado factura:</strong> ${costoTotalFactura.toFixed(2)} €</p>`;

      // Agregar enlace de descarga del CSV
      let csvContent = "data:text/csv;charset=utf-8,";
      csvContent += "Potencia, Número de Residentes, Provincia, Mes, Tipo de Vivienda, Consumo Predicho, Precio Medio, Coste Potencia Contratada, Costo Total Factura\n";
      csvContent += `${potencia}, ${numero_residentes}, ${provincia}, ${mes}, ${tipo_vivienda}, ${consumo}, ${precioMedioKWh}, ${costoPotencia.toFixed(2)}, ${costoTotalFactura.toFixed(2)}\n`;

      const encodedUri = encodeURI(csvContent);
      const downloadLink = document.createElement("a");
      downloadLink.setAttribute("href", encodedUri);
      const imagen = document.createElement("img");
      imagen.src = "../Web_TerraWatt/images/imagen_descarga.png"; // Ajusta la ruta si es necesario
      imagen.alt = "Descargar CSV";
      imagen.style.width = "50px";
      imagen.style.cursor = "pointer";
      imagen.style.alignItems = "center";

      downloadLink.appendChild(imagen);
      resultadoDiv.innerHTML = resultadosHTML + "<br>" + downloadLink.outerHTML;
    })
    .catch((error) => {
      console.error("Error al procesar la solicitud:", error);
      document.getElementById("resultado").innerHTML =
        "<p style='color: red;'>Hubo un error al procesar los datos.</p>";
    });
}
