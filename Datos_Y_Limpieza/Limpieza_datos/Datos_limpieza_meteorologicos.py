# %%
import os
import pandas as pd

# %% [markdown]
# Vamos a empezar eliminando las columnas de nombre e indicativo, ya que hacen referencia a la informacion de la estacion meteorologica ya que como vamos a agruparlos por provinias no es relevante.
# 
# Por otro lado, en los siguientes apartados comprobamos que los archivos son los mismos en ambas carpetas, tanto en la d elos datos brutos como en los datos limpios, para verificar que no haya ningun error.
# 
# En primera instancia, obtenemos que no tineen el mismo número de archivos, ya que en el archivo indicado en el código siguiente tiene una errata en una de las líneas, por lo que la eliminamos y volvemos a procesar todo para verificar que ahora tenemos el mismo número de archivos. 

# %% [markdown]
# **Estas son las conclusiones que sacamos cuando nos dimos cuenta que este archivo era problemático**
# 
# Como podemos ver el error es en el archivo 1111X-20120301-20241103.csv, por lo que vamos a proceser a analizarlo.
# 
# Vemos que el error se encuentra en la linea 4474 ya que se muetsra de la siguiente manera: 
# 
# 2024-05-29;1111X;SANTANDER;CANTABRIA;51;16.0;4.6;13.7;05:002024-05-03;1111X;SANTANDER;CANTABRIA;51;14.1;Ip;9.7;05:20;18.5;23:59;07;5.0;11.7;16:20;8.5;1011.2;08;1004.1;24
# 
# 
# Como podemos ver es una errata, por lo que primeramente eliminaremos esta fila y actoseguido realizaremos el proceso de nuevo. Volveremos a ejecutar todo de nuevo comprobando otra vez que el número de archivos esta ves si es igual.

# %%
# Definimos de donde vamos a obtener los datos y donde vamos a guardar los datos limpios 
input_folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_brutos_meteorologicos"  
output_folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos"  

# Crear la carpeta de salida si no existe
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Procesar cada archivo CSV en la carpeta de entrada
for file_name in os.listdir(input_folder):
    if file_name.endswith(".csv"):  # Verificar que sea un archivo CSV
        input_path = os.path.join(input_folder, file_name)

        try:
            # Eliminamos una linea de este archivo ya que hay una errata y no nos permite limpar bien el 
            # documento (la comprobación esta en el siguiente apartado)
            if file_name == "1111X-20120301-20241103.csv":
                # Leer todas las líneas del archivo
                with open(input_path, 'r', encoding='latin1') as file:
                    lines = file.readlines()
                # Eliminar la línea problemática 
                problematic_line_index = 4473  
                if len(lines) > problematic_line_index:
                    del lines[problematic_line_index]
                    print(f"Línea problemática eliminada en el archivo {file_name}")
                else:
                    print(f"El archivo {file_name} tiene menos de {problematic_line_index + 1} líneas.")
                # Escribir las líneas corregidas a un archivo temporal
                temp_input_path = os.path.join(input_folder, 'temp_' + file_name)
                with open(temp_input_path, 'w', encoding='latin1') as file:
                    file.writelines(lines)
                # Leer el archivo temporal con pandas
                df = pd.read_csv(temp_input_path, delimiter=';', encoding='latin1', engine='python')
                # Eliminar el archivo temporal
                os.remove(temp_input_path)
            else:
                
                df = pd.read_csv(input_path, delimiter=';', encoding='latin1', engine='python')
            
            df.columns = df.columns.str.strip()  # Quitar espacios en los nombres de columnas
            
            # Eliminar las columnas 'INDICATIVO' y 'NOMBRE', si existen
            columns_to_drop = [col for col in ['INDICATIVO', 'NOMBRE'] if col in df.columns]
            df = df.drop(columns=columns_to_drop)
            
            # Guardar el archivo  en la carpeta de salida
            output_path = os.path.join(output_folder, file_name)
            df.to_csv(output_path, sep=';', index=False, encoding='latin1')
        
        except Exception as e:
            print(f"Error procesando el archivo {input_path}: {e}")

print("Procesamiento completado.")

# %% [markdown]
# Comprobamos que los tenemos el mismo número de archivos en ambas carpetas para verificar que el proceso se ha realizado correctamente. Si existe algún problema, nos dirá el nombre de los archivos faltante o extras en ambas carpetas. 

# %%
# Definimos las carpetas
input_folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_brutos_meteorologicos"  
output_folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos" 

# Obtener listas de archivos en ambas carpetas
input_files = set(os.listdir(input_folder))
output_files = set(os.listdir(output_folder))

# Contar archivos en cada carpeta
num_input_files = len(input_files)
num_output_files = len(output_files)

# Mostrar el número de archivos en cada carpeta
print(f"Número de archivos en '{input_folder}': {num_input_files}")
print(f"Número de archivos en '{output_folder}': {num_output_files}")

# Comparar los archivos
missing_files = input_files - output_files

# Mostrar los archivos que faltan en la carpeta NUEVO
if len(missing_files) == 0 and num_input_files == num_output_files:
    print("Todas las carpetas tienen los mismos archivos con el mismo nombre.")
else:
    print(f"Hay {len(missing_files)} archivos que faltan en la carpeta '{output_folder}':")
    for file_name in missing_files:
        print(file_name)

# %% [markdown]
# Vamos a proceder a obtener el nombre de las provincias, por si alguna duplicada o con acrónimos diferentes, por ejemplo Comunidad y C. para no tener archivos duplicados cuando los unamos en base a las provincias.

# %%
# Definir la carpeta de entrada
input_folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos"

provincias_unicas = set()

# Procesar cada archivo CSV en la carpeta
for file_name in os.listdir(input_folder):
    if file_name.endswith(".csv"):  
        input_path = os.path.join(input_folder, file_name)
        
        try:
            df = pd.read_csv(input_path, delimiter=';', encoding='latin1', engine='python')
            
            # Verificar si existe la columna "PROVINCIA"
            if "PROVINCIA" in df.columns:
                # Agregar las provincias únicas al conjunto
                provincias_unicas.update(df["PROVINCIA"].dropna().unique())
            else:
                print(f"El archivo {file_name} no contiene la columna 'PROVINCIA'")
        
        except Exception as e:
            print(f"Error procesando el archivo {file_name}: {e}")

# Mostrar las provincias únicas
print("\nProvincias únicas encontradas:")
for provincia in sorted(provincias_unicas):
    print(provincia)

# %% [markdown]
# Como podemos ver en este caso obtenemos dos Santa cruz de tenerife ya que en una de ellas se usa el acronimo sta. y por otro lado vemos que la provincia de alava esta etiquetada como Araba/alava Por lo que vamos a actualizarlo 

# %% [markdown]
# Por otro lado, como tenemos datos que son dese 1980 vamos a eliminar todos los datos anteriores a 2014, ya que los datos con los que contamos del precio de la elictricidad no son más antiguos de eso. 

# %%
import os
import pandas as pd

# Definir las carpetas de entrada y salida
folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos"
# Crear la carpeta de salida si no existe
if not os.path.exists(folder):
    os.makedirs(folder)

# Recorrer todos los archivos en la carpeta de entrada
for file_name in os.listdir(folder):
    if file_name.endswith(".csv"):  # Verificar que sea un archivo CSV
        input_path = os.path.join(folder, file_name)

        try:
            # Leer el archivo CSV
            df = pd.read_csv(input_path, delimiter=';', encoding='latin1', engine='python')

            # Reemplazar valores incorrectos en la columna "PROVINCIA"
            if "PROVINCIA" in df.columns:
                df["PROVINCIA"] = df["PROVINCIA"].replace({
                    "ARABA/ALAVA": "ALAVA",
                    "STA. CRUZ DE TENERIFE": "SANTA CRUZ DE TENERIFE",
                    "BALEARES": "ILLES BALEARS"
                })

            # Verificar que la columna FECHA existe
            if "FECHA" in df.columns:
                # Convertir la columna FECHA a formato datetime para filtrado
                df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y-%m-%d", errors="coerce")

                # Filtrar las filas con fechas posteriores al 1 de abril de 2014
                df = df[df["FECHA"] >= "2014-04-01"]

            # Guardar el archivo procesado
            output_path = os.path.join(folder, file_name)
            df.to_csv(output_path, sep=';', index=False, encoding="latin1")

        except Exception as e:
            print(f"Error procesando el archivo {input_path}: {e}")

print("Procesamiento completado.")

# %% [markdown]
# Vamos a simplificar los datos por provincia y por fecha, haciendo la media de los valores en función de la provincia, es decir si contamos con 10 estaciones meteorologicas en Guadalajara, se hará la media diaria de todos los valores que existen dentro de esta provincia, siempre segmentados por días.
# 
# Además sustituimos todos estos datasets y los sustituimos por uno independiente por cada provincia, el nombre de cada archivo será el correspondiente a cada provincia.

# %%
folder = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos"

if not os.path.exists(folder):
    os.makedirs(folder)

# Crear un diccionario para almacenar los DataFrames por provincia
dataframes_by_province = {}
generated_files = []  # Lista para almacenar los nombres de los archivos consolidados

# Leer todos los archivos y agruparlos por provincia
for file_name in os.listdir(folder):
    if file_name.endswith(".csv"):  # Verificar que sea un archivo CSV
        input_path = os.path.join(folder, file_name)
        try:
            # Leer el archivo CSV
            df = pd.read_csv(input_path, delimiter=';', encoding='latin1', engine='python')
            
            # Asegurar que las columnas necesarias existen
            if {"PROVINCIA", "FECHA"}.issubset(df.columns):
                # Convertir la columna FECHA a formato datetime para agrupar
                df["FECHA"] = pd.to_datetime(df["FECHA"], format="%Y-%m-%d", errors="coerce")
                
                # Agregar los datos al diccionario por provincia
                for provincia in df["PROVINCIA"].unique():
                    if provincia not in dataframes_by_province:
                        dataframes_by_province[provincia] = []
                    dataframes_by_province[provincia].append(df[df["PROVINCIA"] == provincia])
            else:
                print(f"El archivo {file_name} no tiene las columnas necesarias.")
        
        except Exception as e:
            print(f"Error leyendo archivo {file_name}: {e}")

# Procesar los datos de cada provincia
for provincia, dfs in dataframes_by_province.items():
    print(f"Procesando provincia: {provincia}")

    try:
        # Combinar todos los DataFrames de la provincia
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Agrupar por fecha y calcular la media de las columnas numéricas
        aggregated_df = combined_df.groupby("FECHA").mean(numeric_only=True).reset_index()
        
        # Guardar el archivo consolidado para la provincia
        output_file_name = f"{provincia.replace('/', '_')}.csv"
        output_path = os.path.join(folder, output_file_name)  # Reemplazar '/' por '_' en nombres de archivos
        aggregated_df.to_csv(output_path, sep=';', index=False, encoding='latin1')
        print(f"Archivo consolidado guardado: {output_path}")
        
        # Registrar el archivo generado
        generated_files.append(output_file_name)
    
    except Exception as e:
        print(f"Error procesando la provincia {provincia}: {e}")

# Eliminar los archivos originales que no están en la lista de archivos generados
for file_name in os.listdir(folder):
    if file_name not in generated_files and file_name.endswith(".csv"):
        file_path = os.path.join(folder, file_name)
        os.remove(file_path)
        print(f"Archivo eliminado: {file_path}")

print("Consolidación y limpieza completadas.")

# %% [markdown]
# Por otro lado, nos vemos obligados a eliminar las ciudades de Ceuta y Melilla, ya que los datos que nos han proporcionado sobre el consumo no cuentan con datos de estas cuidades autónomas.

# %%
folder_path = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_limpios_meteorologicos"

# Archivos a eliminar
archivos_a_eliminar = ["CEUTA.csv", "MELILLA.csv"]

# Recorrer los archivos en la carpeta y eliminar los especificados
for archivo in archivos_a_eliminar:
    archivo_path = os.path.join(folder_path, archivo)
    if os.path.exists(archivo_path):
        os.remove(archivo_path)
        print(f"Archivo eliminado: {archivo_path}")
    else:
        print(f"Archivo no encontrado: {archivo_path}")

print("Proceso de eliminación completado.")



