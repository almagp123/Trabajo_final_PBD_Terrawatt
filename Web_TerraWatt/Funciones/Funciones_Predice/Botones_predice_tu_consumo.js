// // // NO TENER ENCUENTA SEGURAMENTE HAYA QUE CAMBIARLO TODO CUANDO SE CONECTE AL DOCKER YA QUE YA NO SE PODRÁ USAR UNA FAST API
// // // NO BORRAR PARA TENER DE REFERENCIA EL CÓDIGO!!!!


// document.addEventListener('DOMContentLoaded', () => {
//   console.log("1. DOM completamente cargado, iniciando script");

//   // Seleccionar los elementos
//   const botonesOpciones = document.querySelectorAll('.boton');
//   const seccionDesplegableBase = document.getElementById("seccion-desplegable-base");
//   const avisoTrabajando = document.getElementById("aviso-trabajando");

//   console.log("2. Botones encontrados:", botonesOpciones);
//   console.log("3. Sección desplegable base:", seccionDesplegableBase);
//   console.log("4. Aviso trabajando:", avisoTrabajando);

//   // Verificar si los elementos existen
//   if (botonesOpciones.length === 0) {
//       console.error("ERROR: No se encontraron botones con la clase 'boton'");
//       return;
//   }
//   if (!seccionDesplegableBase) {
//       console.error("ERROR: No se encontró el elemento seccion-desplegable-base");
//       return;
//   }
//   if (!avisoTrabajando) {
//       console.error("ERROR: No se encontró el elemento aviso-trabajando");
//       return;
//   }

//   console.log("5. Todos los elementos encontrados, añadiendo eventos");

//   // Añadir eventos a los botones
//   botonesOpciones.forEach(boton => {
//       console.log("6. Añadiendo evento a botón con id:", boton.id);
//       boton.addEventListener('click', () => {
//           console.log("7. Botón clicado con id:", boton.id);
//           if (boton.id === "opcion-base") {
//               console.log("8. Activando sección desplegable base");
//               seccionDesplegableBase.classList.remove("oculto");
//               seccionDesplegableBase.classList.add("activo");
//               avisoTrabajando.classList.add("oculto");
//               avisoTrabajando.classList.remove("activo");
//               console.log("9. Clases de seccion-desplegable-base después del cambio:", seccionDesplegableBase.className);
//           } else if (boton.id === "opcion-valle") {
//               console.log("10. Activando aviso trabajando");
//               seccionDesplegableBase.classList.add("oculto");
//               seccionDesplegableBase.classList.remove("activo");
//               avisoTrabajando.classList.remove("oculto");
//               avisoTrabajando.classList.add("activo");
//               console.log("11. Clases de aviso-trabajando después del cambio:", avisoTrabajando.className);
//           }
//       });
//   });

//   // Función genérica para manejar incrementos y decrementos (sin logs para mantenerlo simple)
//   function manejarIncrementoDecremento(decrementBtn, incrementBtn, input, step = 1) {
//       decrementBtn.addEventListener("click", () => {
//           const currentValue = parseFloat(input.value);
//           const minValue = parseFloat(input.min);
//           if (currentValue > minValue) {
//               input.value = (currentValue - step).toFixed(1);
//           }
//       });

//       incrementBtn.addEventListener("click", () => {
//           const currentValue = parseFloat(input.value);
//           const maxValue = parseFloat(input.max);
//           if (currentValue < maxValue) {
//               input.value = (currentValue + step).toFixed(1);
//           }
//       });
//   }

//   // Manejo de los controles de incremento/decremento
//   manejarIncrementoDecremento(
//       document.getElementById("decrement"),
//       document.getElementById("increment"),
//       document.getElementById("potencia"),
//       0.5
//   );

//   manejarIncrementoDecremento(
//       document.getElementById("decrement-residentes"),
//       document.getElementById("increment-residentes"),
//       document.getElementById("numero_residentes"),
//       1
//   );

//   // Función para enviar datos (sin logs para mantenerlo simple)
//   function enviarDatos() {
//       const potencia = parseFloat(document.getElementById("potencia").value);
//       console.log("Potencia capturada como número:", potencia);

//       const numero_residentes = parseInt(document.getElementById("numero_residentes").value, 10);
//       const tipo_vivienda = document.getElementById("tipo_vivienda").value;
//       const provincia = document.getElementById("provincia").value;
//       const mes = parseInt(document.getElementById("mes").value);

//       const datos = {
//           potencia: potencia,
//           numero_residentes: numero_residentes,
//           tipo_vivienda: tipo_vivienda,
//           provincia: provincia,
//           mes: mes
//       };
//       console.log("Datos enviados al backend:", datos);

//       fetch("http://127.0.0.1:8000/transformar", {
//           method: "POST",
//           headers: { "Content-Type": "application/json" },
//           body: JSON.stringify(datos),
//       })
//       .then(response => response.json())
//       .then(data => {
//           console.log("Respuesta recibida del backend:", data);
//           const resultadoDiv = document.getElementById("resultado");
//           const transformados = data.datos_transformados;

//           if (!transformados) {
//               console.error("Error: No se encontraron datos transformados en la respuesta del backend.");
//               resultadoDiv.innerHTML = "<p style='color: red;'>Error al obtener datos transformados.</p>";
//               return;
//           }
//           console.log("Datos transformados:", transformados);

//           const consumo = transformados.prediccion_consumo;
//           const consumoFormateado = consumo.toFixed(2) + " kWh";
//           let precioMedioKWh = 0;
//           if (transformados.precio && typeof transformados.precio === "object") {
//               precioMedioKWh = (transformados.precio.precio_medio / 1000).toFixed(4);
//           }

//           const dias = 30;
//           const costoPotencia = potencia * precioMedioKWh * dias;
//           const costoEnergia = consumo * precioMedioKWh;
//           const costoTotalFactura = parseFloat(costoEnergia) + parseFloat(costoPotencia);

//           let resultadosHTML = `<p><strong>Consumo predicho:</strong> ${consumoFormateado}</p>`;
//           resultadosHTML += `<p><strong>Precio medio:</strong> ${precioMedioKWh} €/kWh</p>`;
//           resultadosHTML += `<p><strong>Coste potencia contratada:</strong> ${costoPotencia.toFixed(2)} €</p>`;
//           resultadosHTML += `<p><strong>Costo total estimado factura:</strong> ${costoTotalFactura.toFixed(2)} €</p>`;

//           let csvContent = "data:text/csv;charset=utf-8,";
//           csvContent += "Potencia, Número de Residentes, Provincia, Mes, Tipo de Vivienda, Consumo Predicho, Precio Medio, Coste Potencia Contratada, Costo Total Factura\n";
//           csvContent += `${potencia}, ${numero_residentes}, ${provincia}, ${mes}, ${tipo_vivienda}, ${consumo}, ${precioMedioKWh}, ${costoPotencia.toFixed(2)}, ${costoTotalFactura.toFixed(2)}\n`;

//           const encodedUri = encodeURI(csvContent);
//           const downloadLink = document.createElement("a");
//           downloadLink.setAttribute("href", encodedUri);
//           const imagen = document.createElement("img");
//           imagen.src = "../Web_TerraWatt/images/imagen_descarga.png";
//           imagen.alt = "Descargar CSV";
//           imagen.style.width = "50px";
//           imagen.style.cursor = "pointer";
//           imagen.style.alignItems = "center";

//           downloadLink.appendChild(imagen);
//           resultadoDiv.innerHTML = resultadosHTML + "<br>" + downloadLink.outerHTML;
//       })
//       .catch(error => {
//           console.error("Error al procesar la solicitud:", error);
//           document.getElementById("resultado").innerHTML = "<p style='color: red;'>Hubo un error al procesar los datos.</p>";
//       });
//   }
// });



// Definir enviarDatos en el ámbito global
window.enviarDatos = function() {
  console.log("12. Iniciando enviarDatos");

  const potencia = parseFloat(document.getElementById("potencia").value);
  console.log("13. Potencia capturada:", potencia);

  const numero_residentes = parseInt(document.getElementById("numero_residentes").value, 10);
  const tipo_vivienda = document.getElementById("tipo_vivienda").value;
  const provincia = document.getElementById("provincia").value;
  const mes = parseInt(document.getElementById("mes").value);

  console.log("14. Valores capturados:", { potencia, numero_residentes, tipo_vivienda, provincia, mes });

  // Validar que los valores no estén vacíos
  if (!potencia || isNaN(numero_residentes) || !tipo_vivienda || !provincia || isNaN(mes)) {
      console.error("15. Error: Faltan datos requeridos");
      document.getElementById("resultado").innerHTML = "<p style='color: red;'>Por favor, completa todos los campos.</p>";
      return;
  }

  const datos = {
      potencia: potencia,
      numero_residentes: numero_residentes,
      tipo_vivienda: tipo_vivienda,
      provincia: provincia,
      mes: mes
  };
  console.log("16. Datos enviados al backend:", datos);

  fetch("http://127.0.0.1:8000/transformar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(datos),
  })
  .then(response => {
      console.log("17. Respuesta recibida del servidor:", response);
      if (!response.ok) {
          throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.json();
  })
  .then(data => {
      console.log("18. Datos procesados del backend:", data);
      const resultadoDiv = document.getElementById("resultado");
      const transformados = data.datos_transformados;

      if (!transformados) {
          console.error("19. Error: No se encontraron datos transformados en la respuesta del backend.");
          resultadoDiv.innerHTML = "<p style='color: red;'>Error al obtener datos transformados.</p>";
          return;
      }
      console.log("20. Datos transformados:", transformados);

      const consumo = transformados.prediccion_consumo;
      const consumoFormateado = consumo.toFixed(2) + " kWh";
      let precioMedioKWh = 0;
      if (transformados.precio && typeof transformados.precio === "object") {
          precioMedioKWh = (transformados.precio.precio_medio / 1000).toFixed(4);
      }

      const dias = 30;
      const costoPotencia = potencia * precioMedioKWh * dias;
      const costoEnergia = consumo * precioMedioKWh;
      const costoTotalFactura = parseFloat(costoEnergia) + parseFloat(costoPotencia);

      let resultadosHTML = `<p><strong>Consumo predicho:</strong> ${consumoFormateado}</p>`;
      resultadosHTML += `<p><strong>Precio medio:</strong> ${precioMedioKWh} €/kWh</p>`;
      resultadosHTML += `<p><strong>Coste potencia contratada:</strong> ${costoPotencia.toFixed(2)} €</p>`;
      resultadosHTML += `<p><strong>Costo total estimado factura:</strong> ${costoTotalFactura.toFixed(2)} €</p>`;

      let csvContent = "data:text/csv;charset=utf-8,";
      csvContent += "Potencia, Número de Residentes, Provincia, Mes, Tipo de Vivienda, Consumo Predicho, Precio Medio, Coste Potencia Contratada, Costo Total Factura\n";
      csvContent += `${potencia}, ${numero_residentes}, ${provincia}, ${mes}, ${tipo_vivienda}, ${consumo}, ${precioMedioKWh}, ${costoPotencia.toFixed(2)}, ${costoTotalFactura.toFixed(2)}\n`;

      const encodedUri = encodeURI(csvContent);
      const downloadLink = document.createElement("a");
      downloadLink.setAttribute("href", encodedUri);
      const imagen = document.createElement("img");
      imagen.src = "../Web_TerraWatt/images/imagen_descarga.png";
      imagen.alt = "Descargar CSV";
      imagen.style.width = "50px";
      imagen.style.cursor = "pointer";
      imagen.style.alignItems = "center";

      downloadLink.appendChild(imagen);
      resultadoDiv.innerHTML = resultadosHTML + "<br>" + downloadLink.outerHTML;
      console.log("21. Resultados mostrados en el DOM");
  })
  .catch(error => {
      console.error("22. Error al procesar la solicitud:", error);
      document.getElementById("resultado").innerHTML = "<p style='color: red;'>Hubo un error al conectar con la API: " + error.message + "</p>";
  });
};

// Código para manejar el desplegable y los botones de incremento/decremento
document.addEventListener('DOMContentLoaded', () => {
  console.log("1. DOM completamente cargado, iniciando script");

  // Seleccionar los elementos
  const botonesOpciones = document.querySelectorAll('.boton');
  const seccionDesplegableBase = document.getElementById("seccion-desplegable-base");
  const avisoTrabajando = document.getElementById("aviso-trabajando");

  console.log("2. Botones encontrados:", botonesOpciones);
  console.log("3. Sección desplegable base:", seccionDesplegableBase);
  console.log("4. Aviso trabajando:", avisoTrabajando);

  // Verificar si los elementos existen
  if (botonesOpciones.length === 0) {
      console.error("ERROR: No se encontraron botones con la clase 'boton'");
      return;
  }
  if (!seccionDesplegableBase) {
      console.error("ERROR: No se encontró el elemento seccion-desplegable-base");
      return;
  }
  if (!avisoTrabajando) {
      console.error("ERROR: No se encontró el elemento aviso-trabajando");
      return;
  }

  console.log("5. Todos los elementos encontrados, añadiendo eventos");

  // Añadir eventos a los botones
  botonesOpciones.forEach(boton => {
      console.log("6. Añadiendo evento a botón con id:", boton.id);
      boton.addEventListener('click', () => {
          console.log("7. Botón clicado con id:", boton.id);
          if (boton.id === "opcion-base") {
              console.log("8. Activando sección desplegable base");
              seccionDesplegableBase.classList.remove("oculto");
              seccionDesplegableBase.classList.add("activo");
              avisoTrabajando.classList.add("oculto");
              avisoTrabajando.classList.remove("activo");
              console.log("9. Clases de seccion-desplegable-base después del cambio:", seccionDesplegableBase.className);
          } else if (boton.id === "opcion-valle") {
              console.log("10. Activando aviso trabajando");
              seccionDesplegableBase.classList.add("oculto");
              seccionDesplegableBase.classList.remove("activo");
              avisoTrabajando.classList.remove("oculto");
              avisoTrabajando.classList.add("activo");
              console.log("11. Clases de aviso-trabajando después del cambio:", avisoTrabajando.className);
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
});