import requests
import json
import pandas as pd
from datetime import datetime, timedelta

# Definir los encabezados para la solicitud
headers = {
    'accept': 'application/json; application/vnd.esios-api-v1+json',
    'accept-language': 'en-US,en;q=0.9,es;q=0.8',
    'content-type': 'application/json',
    'origin': 'https://www.esios.ree.es',
    'referer': 'https://www.esios.ree.es/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-api-key': '7d3f64408f1761c249267d044924fb41d7e5aa73303c9875747a8b02258c7fd6'
}

# Configurar las tasas de impuestos
tasa_impuesto_electrico = 0.0511269632
tasa_iva = 0.21

# Configurar la fecha de inicio y fin
fecha_inicio = datetime(2024, 3, 31)
fecha_fin = datetime(2024, 11, 3)

# Lista para almacenar los resultados
resultados = []

# Bucle para cada día en el rango de fechas
fecha_actual = fecha_inicio
while fecha_actual <= fecha_fin:
    fecha_str = fecha_actual.strftime("%Y-%m-%d")
    url = f"https://api.esios.ree.es/archives/70/download_json?locale=es&date={fecha_str}"
    
    # Realizar la solicitud
    # response = requests.get(url, headers=headers)
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener datos para {fecha_str}: {e}")
        fecha_actual += timedelta(days=1)
    continue

    
    # Verificar que la respuesta sea exitosa
    if response.status_code == 200:
        data = response.json()
        
        # Comprobar si la clave "PVPC" está en los datos
        if "PVPC" in data:
            # Diferenciar entre datos antes y después del 31/05/2021
            if fecha_actual < datetime(2021, 6, 1):
                # Antes de 31/05/2021 - Utilizar "PMHGEN" para calcular el promedio
                precios_horarios = [float(entry["PMHGEN"].replace(",", ".")) for entry in data["PVPC"] if "PMHGEN" in entry]
            else:
                # A partir del 01/06/2021 - Usar "PCB" (Precio de Consumo Base) para el cálculo
                precios_horarios = [float(entry["PCB"].replace(",", ".")) for entry in data["PVPC"] if "PCB" in entry]
            
            # Comprobar si se encontraron precios horarios
            if precios_horarios:
                # Calcular el precio medio diario sin impuestos
                precio_medio_diario_sin_impuestos = sum(precios_horarios) / len(precios_horarios)
                
                # Calcular los impuestos
                impuesto_electrico = precio_medio_diario_sin_impuestos * tasa_impuesto_electrico
                iva = (precio_medio_diario_sin_impuestos + impuesto_electrico) * tasa_iva
                
                # Almacenar los resultados en el diccionario con una única columna "Fecha"
                resultados.append({
                    "Fecha": fecha_str,
                    "Precio medio diario sin impuestos (€/MWh)": precio_medio_diario_sin_impuestos,
                    "Impuesto eléctrico (€/MWh)": impuesto_electrico,
                    "IVA (€/MWh)": iva,
                    "Precio total con impuestos (€/MWh)": precio_medio_diario_sin_impuestos + impuesto_electrico + iva
                })
            else:
                print(f"No se encontraron valores para el precio en {fecha_str}")
        else:
            print(f"No se encontraron datos de PVPC para {fecha_str}")
    else:
        print(f"Error al obtener datos para {fecha_str}: {response.status_code}")
    
    # Avanzar al siguiente día
    fecha_actual += timedelta(days=1)

# Convertir la lista de resultados a un DataFrame de pandas
df_resultados = pd.DataFrame(resultados)

# Mostrar el DataFrame
df_resultados

import os

# Definir la ruta de la carpeta y el archivo
folder_path = '../TerraWatt/Terrawatt/Limpieza_datos/Datos_brutos_generales'
file_path = os.path.join(folder_path, 'precios_energia1.csv')

# Crear la carpeta si no existe
os.makedirs(folder_path, exist_ok=True)

# Guardar el DataFrame en un archivo CSV
df_resultados.to_csv(file_path, index=False, encoding='utf-8')

print(f"Archivo guardado en {file_path}")

