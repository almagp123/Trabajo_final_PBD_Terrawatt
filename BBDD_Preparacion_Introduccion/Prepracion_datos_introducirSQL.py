import pandas as pd

# Ruta del archivo (asegúrate que la ruta sea correcta)
file_path = "../TerraWatt/Terrawatt/Limpieza_datos/Modelo_Precios_Met_Fest.csv"

# Leer el CSV
df_final = pd.read_csv(file_path, delimiter=';')

# Añadir columna ID autoincremental
df_final.insert(0, "ID_precios", range(1, len(df_final) + 1))

# Renombrar columnas con caracteres problemáticos
df_final.rename(columns={
    "Precio total con impuestos (€/MWh)": "Precio_total_con_impuestos", 
    "Entre semana": "Entre_semana",
}, inplace=True)

# Guardar el nuevo CSV con ID
output_path = "../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_precios_SQL_ID.csv"
df_final.to_csv(output_path, sep=';', index=False, encoding='utf-8')

print(f"Archivo combinado guardado en: {output_path}")
print("🔍 ¿Hay NaNs en el DataFrame?", df_final.isnull().sum().sum(), "valores nulos")
nan_rows = df_final[df_final.isnull().any(axis=1)]
print("🔍 Filas con valores faltantes:")
print(nan_rows.loc[:, nan_rows.isnull().any()])
print(f"\n🧮 Total de filas con NaN: {len(nan_rows)}")





# ---------------------------
import os
import pandas as pd

# Ruta a la carpeta donde están todos los CSV
carpeta = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_consumo_generados_meteorologicos"

# Lista para acumular los DataFrames
dataframes = []

# Recorremos todos los archivos en la carpeta
for archivo in os.listdir(carpeta):
    if archivo.endswith(".csv"):
        ruta = os.path.join(carpeta, archivo)
        print(f"Procesando archivo: {archivo}")
        
        # Leer con separador coma (CSV original con ,)
        df = pd.read_csv(ruta, delimiter=',', encoding='utf-8', low_memory=False)

        # Limpiar nombres de columnas
        df.columns = [col.strip() for col in df.columns]

        dataframes.append(df)

# Unir todos los DataFrames
df_final = pd.concat(dataframes, ignore_index=True)
df_final.rename(columns={
    "Consumo energético (kWh/m²)": "Consumo_energetico_kWh_m2",
    "Media de residentes": "Media_de_residentes",
    "Potencia contratada (kW)": "Potencia_contratada_kW",
    "Tipo de vivienda": "Tipo_de_vivienda",
}, inplace=True)

# Añadir columna ID autoincremental
df_final.insert(0, 'ID_consumo', range(1, len(df_final) + 1))

# Guardar el archivo combinado con separador ;
salida = "../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_consumo_SQL_ID.csv"
df_final.to_csv(salida, sep=';', index=False, encoding='utf-8')

print(f"\n✅ Archivo final guardado en: {salida}")

provincias = sorted(df_final['Provincia'].unique())
for p in provincias:
    print(f"('{p}'),")
