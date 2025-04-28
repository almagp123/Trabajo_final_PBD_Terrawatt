import pandas as pd
import os

# --- Rutas de los archivos ---
meteorologicos_path = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_limpios/Datos_limpios_metereologicos"
precios_path = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_brutos/Precios_energia.csv"
festivos_path = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_brutos/Festivos.csv"
output_path = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_limpios/Modelo_Precios_Met_Fest.csv"

# --- Paso 1: Convertir todos los meteorológicos a UTF-8 ---
for file_name in os.listdir(meteorologicos_path):
    if file_name.endswith(".csv"):
        file_path = os.path.join(meteorologicos_path, file_name)
        
        with open(file_path, 'r', encoding='ISO-8859-1') as f:
            contenido = f.read()
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(contenido)
        
        print(f"Archivo convertido a UTF-8: {file_name}")

# --- Paso 2: Leer festivos y precios ---
df_festivos = pd.read_csv(festivos_path, delimiter=';', encoding='utf-8')
df_festivos["Provincia"] = df_festivos["Provincia"].str.strip().str.upper()
df_festivos["Fecha"] = pd.to_datetime(df_festivos["Fecha"], format='%d/%m/%Y', errors='coerce')

df_precios = pd.read_csv(precios_path, delimiter=',', encoding='utf-8')
df_precios["Fecha"] = pd.to_datetime(df_precios["Fecha"], format='%Y-%m-%d', errors='coerce')

# --- Paso 3: Procesar meteorológicos y combinar ---
combined_dfs = []

for file_name in os.listdir(meteorologicos_path):
    if file_name.endswith(".csv"):
        file_path = os.path.join(meteorologicos_path, file_name)
        provincia = os.path.splitext(file_name)[0].upper()
        
        df_meteorologico = pd.read_csv(file_path, delimiter=';', encoding='utf-8')
        df_meteorologico["FECHA"] = pd.to_datetime(df_meteorologico["FECHA"], format='%Y-%m-%d', errors='coerce')
        
        # Añadir columna de Provincia
        df_meteorologico.insert(1, "Provincia", provincia)
        
        # Combinar meteorológicos con precios
        df_combinado = pd.merge(df_meteorologico, df_precios, left_on="FECHA", right_on="Fecha", how="inner")
        
        # Eliminar columna redundante "Fecha"
        df_combinado.drop(columns=["Fecha"], inplace=True)
        
        # Crear columna "Clave" para unir con festivos
        df_combinado["Clave"] = df_combinado["FECHA"].astype(str) + "_" + df_combinado["Provincia"]
        
        # Añadir columna "Festivo"
        df_combinado["Festivo"] = df_combinado["Clave"].isin(
            df_festivos["Fecha"].astype(str) + "_" + df_festivos["Provincia"]
        ).map({True: "SI", False: "NO"})
        
        # Añadir columna "Entre semana"
        df_combinado["Entre semana"] = df_combinado["FECHA"].dt.dayofweek.apply(lambda x: "SI" if x < 5 else "NO")
        
        # Eliminar columna auxiliar "Clave"
        df_combinado.drop(columns=["Clave"], inplace=True)
        
        combined_dfs.append(df_combinado)

# --- Paso 4: Unir todos los datasets ---
df_final = pd.concat(combined_dfs, ignore_index=True)

# --- Paso 5: Seleccionar las columnas deseadas ---
columnas_finales = [
    "FECHA", "Provincia", "ALTITUD", "TMEDIA", "TMIN", "TMAX",
    "DIR", "VELMEDIA", "RACHA", "SOL", "PRESMAX", "PRESMIN",
    "Precio total con impuestos (€/MWh)", "Festivo", "Entre semana"
]

df_final = df_final[columnas_finales]

# --- Paso 6: Reportar porcentaje de valores nulos por columna ---
print("\n--- Informe de valores faltantes ---")
total_filas = len(df_final)

for col in columnas_finales:
    num_nulos = df_final[col].isnull().sum()
    porcentaje = (num_nulos / total_filas) * 100

    if num_nulos > 0:
        if porcentaje <= 5:
            print(f"La variable '{col}' falta en el {porcentaje:.2f}% de los datos. Se rellenará con la media de su provincia.")
            
            # --- Rellenar valores faltantes con la media de su provincia ---
            def rellenar_con_media_provincia(row):
                if pd.isna(row[col]):
                    provincia = row['Provincia']
                    media = df_final[df_final['Provincia'] == provincia][col].mean()
                    return media
                else:
                    return row[col]

            df_final[col] = df_final.apply(rellenar_con_media_provincia, axis=1)

        else:
            print(f"La variable '{col}' falta en el {porcentaje:.2f}% de los datos. No se rellenará automáticamente (requiere otra estrategia).")
    else:
        print(f"La variable '{col}' no tiene valores faltantes.")

# --- Paso 7: Calcular medias por columna numérica (informativo) ---
numeric_columns = df_final.select_dtypes(include=['number']).columns

print("\n--- Medias de columnas numéricas ---")
for col in numeric_columns:
    mean_value = df_final[col].mean()
    print(f"Columna '{col}': media = {mean_value:.4f}")

# --- Paso 8: Guardar resultado final ---
df_final.to_csv(output_path, sep=';', index=False, encoding='utf-8')
print(f"\nArchivo final guardado en: {output_path}")
