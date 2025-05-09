import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os

# Encabezados para la API de ESIOS
headers = {
    'accept': 'application/json; application/vnd.esios-api-v1+json',
    'accept-language': 'en-US,en;q=0.9,es;q=0.8',
    'content-type': 'application/json',
    'origin': 'https://www.esios.ree.es',
    'referer': 'https://www.esios.ree.es/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'x-api-key': '7d3f64408f1761c249267d044924fb41d7e5aa73303c9875747a8b02258c7fd6'
}

# Impuestos
tasa_impuesto_electrico = 0.0511269632
tasa_iva = 0.21

# Fechas
fecha_inicio = datetime(2014, 4, 1)
fecha_fin = datetime(2024, 4, 1)

# Resultados
resultados = []

fecha_actual = fecha_inicio
while fecha_actual <= fecha_fin:
    fecha_str = fecha_actual.strftime("%Y-%m-%d")
    url = f"https://api.esios.ree.es/archives/70/download_json?locale=es&date={fecha_str}"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error en {fecha_str}: {e}")
        fecha_actual += timedelta(days=1)
        continue

    try:
        data = response.json()
    except json.JSONDecodeError:
        print(f"JSON malformado en {fecha_str}")
        fecha_actual += timedelta(days=1)
        continue

    precios_horarios = []

    # Buscar precios horarios en distintas claves
    if "PVPC" in data:
        entries = data["PVPC"]
    else:
        # Intentar con otras claves (ajusta aquí si encuentras otra estructura)
        keys_disponibles = list(data.keys())
        posible_clave = next((k for k in keys_disponibles if isinstance(data[k], list)), None)
        if posible_clave:
            entries = data[posible_clave]
        else:
            print(f"❌ Sin clave reconocible de precios en {fecha_str}")
            fecha_actual += timedelta(days=1)
            continue

    # Determinar qué campo usar
    if fecha_actual < datetime(2021, 6, 1):
        precios_horarios = [float(e["PMHGEN"].replace(",", ".")) for e in entries if "PMHGEN" in e]
    else:
        precios_horarios = [float(e["PCB"].replace(",", ".")) for e in entries if "PCB" in e]

    if not precios_horarios:
        print(f"⚠️ Sin precios horarios válidos en {fecha_str}")
        fecha_actual += timedelta(days=1)
        continue

    # Cálculos
    precio_medio_diario_sin_impuestos = sum(precios_horarios) / len(precios_horarios)
    impuesto_electrico = precio_medio_diario_sin_impuestos * tasa_impuesto_electrico
    iva = (precio_medio_diario_sin_impuestos + impuesto_electrico) * tasa_iva
    precio_total = precio_medio_diario_sin_impuestos + impuesto_electrico + iva

    resultados.append({
        "Fecha": fecha_str,
        "Precio medio diario sin impuestos (€/MWh)": precio_medio_diario_sin_impuestos,
        "Impuesto eléctrico (€/MWh)": impuesto_electrico,
        "IVA (€/MWh)": iva,
        "Precio total con impuestos (€/MWh)": precio_total
    })

    fecha_actual += timedelta(days=1)

# Exportar a CSV
df_resultados = pd.DataFrame(resultados)
output_dir = '../Datos_Y_Limpieza/Datos_brutos'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, 'precios_energia.csv')
df_resultados.to_csv(output_file, index=False, encoding='utf-8')

print(f"Archivo guardado en: {output_file}")
