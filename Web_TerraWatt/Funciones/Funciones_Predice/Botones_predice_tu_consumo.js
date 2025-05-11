// // Definir enviarDatos en el ámbito global
// window.enviarDatos = function() {

//   const potencia = parseFloat(document.getElementById("potencia").value);

//   const numero_residentes = parseInt(document.getElementById("numero_residentes").value, 10);
//   const tipo_vivienda = document.getElementById("tipo_vivienda").value;
//   const provincia = document.getElementById("provincia").value;
//   const mes = parseInt(document.getElementById("mes").value);

//   console.log("Valores que a introducido el usuario", { potencia, numero_residentes, tipo_vivienda, provincia, mes });

//   // Validar que los valores no estén vacíos
//   if (!potencia || isNaN(numero_residentes) || !tipo_vivienda || !provincia || isNaN(mes)) {
//       console.error("No se han rellenado todos los campos necesarios.");
//       document.getElementById("resultado").innerHTML = "<p style='color: red;'>Por favor, completa todos los campos.</p>";
//       return;
//   }

//   const datos = {
//       potencia: potencia,
//       numero_residentes: numero_residentes,
//       tipo_vivienda: tipo_vivienda,
//       provincia: provincia,
//       mes: mes
//   };

//   fetch("http://127.0.0.1:8000/transformar", {
//       method: "POST",
//       headers: { "Content-Type": "application/json" },
//       body: JSON.stringify(datos),
//   })
//   .then(response => {
//       console.log("Respuesta recibida del servidor:", response);
//       if (!response.ok) {
//           throw new Error(`HTTP error! Status: ${response.status}`);
//       }
//       return response.json();
//   })
//   .then(data => {
//       const resultadoDiv = document.getElementById("resultado");
//       const transformados = data.datos_transformados;

//       if (!transformados) {
//           console.error("No se encontraron datos transformados en la respuesta del backend.");
//           resultadoDiv.innerHTML = "<p style='color: red;'>Error al obtener datos transformados.</p>";
//           return;
//       }

//       const consumo = transformados.prediccion_consumo;
//       const consumoFormateado = consumo.toFixed(2) + " kWh";
//       let precioMedioKWh = 0;
//       if (transformados.precio && typeof transformados.precio === "object") {
//           precioMedioKWh = (transformados.precio.precio_medio / 1000).toFixed(4);
//       }

//       const dias = 30;
//       const costoPotencia = potencia * precioMedioKWh * dias;
//       const costoEnergia = consumo * precioMedioKWh;
//       const costoTotalFactura = parseFloat(costoEnergia) + parseFloat(costoPotencia);

//       let resultadosHTML = `<p><strong>Consumo predicho:</strong> ${consumoFormateado}</p>`;
//       resultadosHTML += `<p><strong>Precio medio:</strong> ${precioMedioKWh} €/kWh</p>`;
//       resultadosHTML += `<p><strong>Coste potencia contratada:</strong> ${costoPotencia.toFixed(2)} €</p>`;
//       resultadosHTML += `<p><strong>Costo total estimado factura:</strong> ${costoTotalFactura.toFixed(2)} €</p>`;

//       let csvContent = "data:text/csv;charset=utf-8,";
//       csvContent += "Potencia, Número de Residentes, Provincia, Mes, Tipo de Vivienda, Consumo Predicho, Precio Medio, Coste Potencia Contratada, Costo Total Factura\n";
//       csvContent += `${potencia}, ${numero_residentes}, ${provincia}, ${mes}, ${tipo_vivienda}, ${consumo}, ${precioMedioKWh}, ${costoPotencia.toFixed(2)}, ${costoTotalFactura.toFixed(2)}\n`;

//       const encodedUri = encodeURI(csvContent);
//       const downloadLink = document.createElement("a");
//       downloadLink.setAttribute("href", encodedUri);

//       const idioma = document.documentElement.lang;
//       let rutaImagen = "/Web_TerraWatt/images/imagen_descarga.png"; 

//       const imagen = document.createElement("img");
//       imagen.src = rutaImagen;
//       imagen.alt = "Descargar CSV";
//       imagen.style.width = "50px";
//       imagen.style.cursor = "pointer";
//       imagen.style.alignItems = "center";

//       downloadLink.appendChild(imagen);
//       resultadoDiv.innerHTML = resultadosHTML + "<br>" + downloadLink.outerHTML;
//   })
//   .catch(error => {
//       document.getElementById("resultado").innerHTML = "<p style='color: red;'>Hubo un error al conectar con la API: " + error.message + "</p>";
//   });
// };

// // Código para manejar el desplegable y los botones de incremento/decremento
// document.addEventListener('DOMContentLoaded', () => {

//   // Seleccionar los elementos
//   const botonesOpciones = document.querySelectorAll('.boton');
//   const seccionDesplegableBase = document.getElementById("seccion-desplegable-base");
//   const avisoTrabajando = document.getElementById("aviso-trabajando");



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


//   // Añadir eventos a los botones
//   botonesOpciones.forEach(boton => {
//       boton.addEventListener('click', () => {
//           if (boton.id === "opcion-base") {
//               seccionDesplegableBase.classList.remove("oculto");
//               seccionDesplegableBase.classList.add("activo");
//               avisoTrabajando.classList.add("oculto");
//               avisoTrabajando.classList.remove("activo");
//           } else if (boton.id === "opcion-valle") {
//               seccionDesplegableBase.classList.add("oculto");
//               seccionDesplegableBase.classList.remove("activo");
//               avisoTrabajando.classList.remove("oculto");
//               avisoTrabajando.classList.add("activo");
//           }
//       });
//   });

//   // Función genérica para manejar incrementos y decrementos
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
// });




// Definir enviarDatos en el ámbito global
window.enviarDatos = function() {
  const potencia = parseFloat(document.getElementById("potencia").value);
  const numero_residentes = parseInt(document.getElementById("numero_residentes").value, 10);
  const tipo_vivienda = document.getElementById("tipo_vivienda").value;
  const provincia = document.getElementById("provincia").value;
  const mes = parseInt(document.getElementById("mes").value);

  console.log("Valores que a introducido el usuario", { potencia, numero_residentes, tipo_vivienda, provincia, mes });

  // Validar que los valores no estén vacíos
  if (!potencia || isNaN(numero_residentes) || !tipo_vivienda || !provincia || isNaN(mes)) {
      console.error("No se han rellenado todos los campos necesarios.");
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

  fetch("http://127.0.0.1:8000/transformar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(datos),
  })
  .then(response => {
      console.log("Respuesta recibida del servidor:", response);
      if (!response.ok) {
          throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.json();
  })
  .then(data => {
      const resultadoDiv = document.getElementById("resultado");
      const transformados = data.datos_transformados;

      if (!transformados) {
          console.error("No se encontraron datos transformados en la respuesta del backend.");
          resultadoDiv.innerHTML = "<p style='color: red;'>Error al obtener datos transformados.</p>";
          return;
      }

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

      // Seleccionar textos según el idioma
      const idioma = document.documentElement.lang;
      let textos = {};
      if (idioma === "en") {
          textos = {
              consumoPredicho: "Predicted consumption",
              precioMedio: "Average price",
              costePotencia: "Contracted power cost",
              costoTotal: "Estimated total bill",
              csvHeaders: "Power, Number of Residents, Province, Month, Type of Dwelling, Predicted Consumption, Average Price, Contracted Power Cost, Estimated Total Bill"
          };
      } else if (idioma === "ar") {
          textos = {
              consumoPredicho: "الاستهلاك المتوقع",
              precioMedio: "السعر المتوسط",
              costePotencia: "تكلفة الطاقة المعاهدة",
              costoTotal: "الفاتورة الإجمالية المتوقعة",
              csvHeaders: "الطاقة, عدد السكان, المحافظة, الشهر, نوع المسكن, الاستهلاك المتوقع, السعر المتوسط, تكلفة الطاقة المعاهدة, الفاتورة الإجمالية المتوقعة"
          };
      } else { // Español por defecto
          textos = {
              consumoPredicho: "Consumo predicho",
              precioMedio: "Precio medio",
              costePotencia: "Coste potencia contratada",
              costoTotal: "Costo total estimado factura",
              csvHeaders: "Potencia, Número de Residentes, Provincia, Mes, Tipo de Vivienda, Consumo Predicho, Precio Medio, Coste Potencia Contratada, Costo Total Factura"
          };
      }

      // Construir resultadosHTML según el idioma
      let resultadosHTML = "";
      if (idioma === "ar") {
          resultadosHTML = `<div dir="rtl" style="text-align: right;">`;
          resultadosHTML += `<p> <strong>  ${textos.consumoPredicho}:</strong> <span style="display: inline-block; direction: ltr;"> ${consumoFormateado}</span></p>`;
          resultadosHTML += `<p> <strong>${textos.precioMedio}:</strong>  <span style="display: inline-block; direction: ltr;"> ${precioMedioKWh} €/kWh</span> </p>`;
          resultadosHTML += `<p> <strong>${textos.costePotencia}:</strong> <span style="display: inline-block; direction: ltr;"> ${costoPotencia.toFixed(2)} €</span></p>`;
          resultadosHTML += `<p> <strong>${textos.costoTotal}:</strong>  <span style="display: inline-block; direction: ltr;"> ${costoTotalFactura.toFixed(2)} €</span></p>`;
          resultadosHTML += `</div>`;
      } else {
          resultadosHTML = `<div>`;
          resultadosHTML += `<p><strong>${textos.consumoPredicho}:</strong> ${consumoFormateado}</p>`;
          resultadosHTML += `<p><strong>${textos.precioMedio}:</strong> ${precioMedioKWh} €/kWh</p>`;
          resultadosHTML += `<p><strong>${textos.costePotencia}:</strong> ${costoPotencia.toFixed(2)} €</p>`;
          resultadosHTML += `<p><strong>${textos.costoTotal}:</strong> ${costoTotalFactura.toFixed(2)} €</p>`;
          resultadosHTML += `</div>`;
      }

      let csvContent = "data:text/csv;charset=utf-8,";
      csvContent += `${textos.csvHeaders}\n`;
      csvContent += `${potencia}, ${numero_residentes}, ${provincia}, ${mes}, ${tipo_vivienda}, ${consumo}, ${precioMedioKWh}, ${costoPotencia.toFixed(2)}, ${costoTotalFactura.toFixed(2)}\n`;

      const encodedUri = encodeURI(csvContent);
      const downloadLink = document.createElement("a");
      downloadLink.setAttribute("href", encodedUri);

      const rutaImagen = "/Web_TerraWatt/images/imagen_descarga.png"; 
      const imagen = document.createElement("img");
      imagen.src = rutaImagen;
      imagen.alt = "Descargar CSV";
      imagen.style.width = "50px";
      imagen.style.cursor = "pointer";
      imagen.style.alignItems = "center";

      downloadLink.appendChild(imagen);
      resultadoDiv.innerHTML = resultadosHTML + "<br>" + downloadLink.outerHTML;
  })
  .catch(error => {
      document.getElementById("resultado").innerHTML = "<p style='color: red;'>Hubo un error al conectar con la API: " + error.message + "</p>";
  });
};

// Código para manejar el desplegable y los botones de incremento/decremento
document.addEventListener('DOMContentLoaded', () => {
  // Seleccionar los elementos
  const botonesOpciones = document.querySelectorAll('.boton');
  const seccionDesplegableBase = document.getElementById("seccion-desplegable-base");
  const avisoTrabajando = document.getElementById("aviso-trabajando");

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

  // Añadir eventos a los botones
  botonesOpciones.forEach(boton => {
      boton.addEventListener('click', () => {
          if (boton.id === "opcion-base") {
              seccionDesplegableBase.classList.remove("oculto");
              seccionDesplegableBase.classList.add("activo");
              avisoTrabajando.classList.add("oculto");
              avisoTrabajando.classList.remove("activo");
          } else if (boton.id === "opcion-valle") {
              seccionDesplegableBase.classList.add("oculto");
              seccionDesplegableBase.classList.remove("activo");
              avisoTrabajando.classList.remove("oculto");
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
});