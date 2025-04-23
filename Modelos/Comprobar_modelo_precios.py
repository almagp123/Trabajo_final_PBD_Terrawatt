import pandas as pd
import numpy as np
from datetime import datetime
import pickle

# 1. Cargar el modelo guardado (asegúrate de que la ruta sea correcta)
modelo_pkl_path = "../TerraWatt/Terrawatt/modelos_guardados/AModelo_precios_mlp.pkl"

# Cargar el modelo
with open(modelo_pkl_path, "rb") as f:
    model = pickle.load(f)

# 2. Cargar el dataset y obtener el último precio
file_path = "../TerraWatt/Terrawatt/Limpieza_datos/Modelo_Precios_Met_Fest.csv"
data = pd.read_csv(file_path, delimiter=';')

# Asegurarse de que no haya valores faltantes en las columnas necesarias
data['FECHA'] = pd.to_datetime(data['FECHA'], errors='coerce')  # Asegura que las fechas sean válidas
data = data.dropna(subset=['FECHA', 'Precio total con impuestos (€/MWh)'])  # Eliminar filas con valores NaN importantes

# Ordenar por fecha
data = data.sort_values(by='FECHA')

# Filtrar datos por provincia (opcional, según necesites)
provincia = "ALAVA"  # Por ejemplo
data_provincia = data[data['Provincia'].str.upper() == provincia.upper()]
if data_provincia.empty:
    raise ValueError(f"No se encontraron datos para la provincia {provincia}")

# Obtener el último precio de la columna de interés
ultimo_precio = data_provincia['Precio total con impuestos (€/MWh)'].values[-1]
print("Último precio histórico:", ultimo_precio)

# 3. Definir el rango de fechas para el que se desea predecir
# Por ejemplo, enero 2025: desde el 1 al 31 de enero
fecha_inicio = "2025-01-01"
fecha_fin = "2025-01-31"
fecha_inicio_dt = pd.to_datetime(fecha_inicio)
fecha_fin_dt = pd.to_datetime(fecha_fin)

# Generar un rango de fechas diario
rango_fechas = pd.date_range(start=fecha_inicio_dt, end=fecha_fin_dt, freq='D')
n_dias = len(rango_fechas)
print(f"Cantidad de días a predecir: {n_dias}")

# 4. Predicción recursiva: para cada día se predice el siguiente precio
predicciones = []  # para almacenar el precio predicho cada día
precio_actual = ultimo_precio  # punto de partida

for dia in rango_fechas:
    # Preparar la entrada con la forma (1, 1) ya que el modelo fue entrenado con ventanas de tamaño 1
    entrada = np.array([[precio_actual]])
    
    # Asegurarse de que no haya valores NaN en la entrada
    if np.isnan(entrada).any():
        raise ValueError("La entrada contiene valores NaN. No se puede realizar la predicción.")

    precio_siguiente = model.predict(entrada)[0]  # predicción para el siguiente día
    predicciones.append(precio_siguiente)
    # Actualizar el precio_actual con el valor predicho para la siguiente iteración
    precio_actual = precio_siguiente

# 5. Calcular el precio medio en el rango
precio_medio = np.mean(predicciones)
print(f"Precio medio predicho para el intervalo {fecha_inicio} a {fecha_fin} en {provincia}: {precio_medio:.2f} €/MWh")
