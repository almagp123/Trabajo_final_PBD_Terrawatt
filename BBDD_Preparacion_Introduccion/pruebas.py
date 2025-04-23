import pandas as pd
import numpy as np
from datetime import datetime

# Cargar los datos reales
consumo_df = pd.read_csv("../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_consumo_SQL_ID.csv", sep=';')
precios_df = pd.read_csv("../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_precios_SQL_ID.csv", sep=';')

# Normalizar nombres de columnas
consumo_df.columns = consumo_df.columns.str.strip().str.replace(" ", "_")
precios_df.columns = precios_df.columns.str.strip().str.replace(" ", "_")

# Convertir fechas
consumo_df["Fecha"] = pd.to_datetime(consumo_df["Fecha"], format="%Y-%m", errors='coerce')
precios_df["FECHA"] = pd.to_datetime(precios_df["FECHA"], errors='coerce')

# Crear columna MES
precios_df["MES"] = precios_df["FECHA"].dt.to_period("M").astype(str)

# Media de precios diarios por provincia y mes
precio_mensual = precios_df.groupby(["Provincia", "MES"])["Precio_total_con_impuestos"].mean().reset_index()

# Unir consumo con precio medio por provincia (para generar muestras realistas)
merged = consumo_df.merge(precio_mensual, how="inner", left_on=["Provincia"], right_on=["Provincia"])

# Fechas posibles de predicción (como si fueran inputs aleatorios de usuario)
fechas_posibles = pd.date_range(start="2025-03-01", end="2025-10-01", freq="MS").strftime("%Y-%m").tolist()
# Filtrar registros válidos (sin nulos y sin valores fuera de rango)
merged = merged[
    merged["Consumo_energetico_kWh_m2"].notna() &
    merged["Precio_total_con_impuestos"].notna() &
    merged["Precio_total_con_impuestos"] > 0
].reset_index(drop=True)

# Crear muestras sintéticas
np.random.seed(42)
synthetic_samples = []

for i in range(5000):
    row = merged.sample(1).iloc[0]
    
    consumo_real = row["Consumo_energetico_kWh_m2"]
    precio_real = row["Precio_total_con_impuestos"]
    
    consumo_pred = np.round(np.random.uniform(consumo_real * 0.98, consumo_real * 1.02), 2)
    precio_pred = np.round(np.random.uniform(precio_real * 0.98, precio_real * 1.02), 4)
    
    potencia = row["Potencia_contratada_kW"]
    coste_potencia = round(potencia * precio_pred, 2)
    coste_estimado = round(consumo_pred * precio_pred, 2)
    
    mes_aleatorio = np.random.choice(fechas_posibles)
    
    synthetic_samples.append({
        "ID": i + 1,
        "POTENCIA": potencia,
        "NRESIDENTES": row["Media_de_residentes"],
        "TIPOVIVIENDA": row["Tipo_de_vivienda"],
        "PROVINCIA": row["Provincia"],
        "MES": mes_aleatorio,
        "PREDICCION_CONSUMO": consumo_pred,
        "PREDICCION_PRECIO": precio_pred,
        "COSTE_POTENCIA": coste_potencia,
        "COSTE_ESTIMADO": coste_estimado,
        "FECHA_PREDICCION": datetime.now().strftime("%Y-%m-%d")
    })

# Crear el DataFrame y guardarlo
synthetic_df = pd.DataFrame(synthetic_samples)
synthetic_df.to_csv("../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_sinteticos_prediccion.csv", index=False, sep=';')
print("✅ Datos sintéticos generados y guardados como 'Datos_sinteticos_prediccion.csv'")
