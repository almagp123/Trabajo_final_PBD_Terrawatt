import pandas as pd 
import os

# Ruta de los archivos
meteorologicos_path = "../Limpieza_datos/Datos_limpios_meteorologicos"
precios_path = "../Limpieza_datos/Datos_brutos_generales/precios_energia.csv"
festivos_path = "../Limpieza_datos/Datos_brutos_generales/Festivos.csv"

# Leer los festivos
df_festivos = pd.read_csv(festivos_path, delimiter=';', encoding='utf-8')

# Estandarizar las columnas "Provincia" y "Fecha" en el dataset de festivos
df_festivos["Provincia"] = df_festivos["Provincia"].str.strip().str.upper()
df_festivos["Fecha"] = pd.to_datetime(df_festivos["Fecha"], format='%d/%m/%Y', errors='coerce')

# Leer los precios con el delimitador original (coma) y ajustarlo directamente
df_precios = pd.read_csv(precios_path, delimiter=',', encoding='utf-8')

# Convertir la columna "Fecha" a formato datetime en precios
df_precios["Fecha"] = pd.to_datetime(df_precios["Fecha"], format='%Y-%m-%d', errors='coerce')

# Inicializar una lista para almacenar los DataFrames combinados
combined_dfs = []

# Procesar cada archivo en la carpeta de datos meteorológicos
for file_name in os.listdir(meteorologicos_path):
    if file_name.endswith(".csv"):  # Asegurarse de que sean archivos CSV
        file_path = os.path.join(meteorologicos_path, file_name)

        # Extraer el nombre de la provincia del archivo (sin extensión)
        provincia = os.path.splitext(file_name)[0].upper()

        # Leer el archivo meteorológico
        df_meteorologico = pd.read_csv(file_path, delimiter=';', encoding='utf-8')

        # Convertir la columna "FECHA" a formato datetime en meteorológicos
        df_meteorologico["FECHA"] = pd.to_datetime(df_meteorologico["FECHA"], format='%Y-%m-%d', errors='coerce')

        # Añadir la columna Provincia al DataFrame meteorológico
        df_meteorologico.insert(1, "Provincia", provincia)  # Añadir la columna como segunda

        # Combinar con el archivo de precios de energía mediante la columna "Fecha"
        df_combinado = pd.merge(df_meteorologico, df_precios, left_on="FECHA", right_on="Fecha", how="left")

        # Eliminar la columna redundante "Fecha" después de la combinación
        df_combinado.drop(columns=["Fecha"], inplace=True)

        # Crear una clave de combinación basada en Fecha y Provincia para identificar festivos
        df_combinado["Clave"] = df_combinado["FECHA"].astype(str) + "_" + df_combinado["Provincia"]

        # Añadir la columna "Festivo"
        df_combinado["Festivo"] = df_combinado["Clave"].isin(
            df_festivos["Fecha"].astype(str) + "_" + df_festivos["Provincia"]
        ).map({True: "SI", False: "NO"})

        # Añadir la columna "Entre semana"
        df_combinado["Entre semana"] = df_combinado["FECHA"].dt.dayofweek.apply(lambda x: "SI" if x < 5 else "NO")

        # Eliminar la columna auxiliar "Clave"
        df_combinado.drop(columns=["Clave"], inplace=True)

        # Añadir al listado de DataFrames combinados
        combined_dfs.append(df_combinado)

# Concatenar todos los DataFrames combinados
df_final = pd.concat(combined_dfs, ignore_index=True)

# Dropear las columnas de precios que no sean necesarias
columns_to_drop = [
    "Precio medio diario sin impuestos (€/MWh)",
    "Impuesto eléctrico (€/MWh)",
    "IVA (€/MWh)"
]
df_final.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Guardar el archivo combinado
output_path = "../TerraWatt/Terrawatt/Limpieza_datos/Modelo_Precios_Met_Fest.csv"
# output_path = "Modelo_Precios_Met_Fest.csv"

df_final.to_csv(output_path, sep=';', index=False, encoding='utf-8')

print(f"Archivo combinado guardado en: {output_path}")


# Debemos de rellenar todas las filas que no contengan datos, en este caso lo rellenaremos con la media de la columna (ya que el unico dataset que tiene datos faltantes es el de datos metereologicos), paorque elastic search no admite que tenga elementos faltantes.


# Lista de archivos a procesar
files = [ "../TerraWatt/Terrawatt/Limpieza_datos/Modelo_Precios_Met_Fest.csv"]

# Procesar cada archivo
for file in files:
    # Leer el archivo CSV
    df = pd.read_csv(file, delimiter=';')
    
    # Eliminar filas donde todas las columnas tengan valores faltantes
    df.dropna(how='all', inplace=True)
    
    # Reemplazar valores faltantes en columnas numéricas con la media de la columna
    numeric_columns = df.select_dtypes(include=['number']).columns
    for col in numeric_columns:
        mean_value = df[col].mean()  # Calcular la media de la columna
        df[col].fillna(mean_value)
    
    # Guardar el archivo procesado con un prefijo "Procesado_"
    output_file = f"{file}"
    df.to_csv(output_file, index=False, sep=';')
    print(f"Archivo procesado y guardado como: {output_file}")