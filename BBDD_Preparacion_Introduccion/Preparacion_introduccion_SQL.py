import os
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

#Credenciales de acceso a la base de datos
user = "uhmzmxoizkatmdsu"
password = "hcG4aHLWkwV4KrjM9re"
host = "hv-par8-022.clvrcld.net"
port = "10532"
database = "brqtr1tzuvatzxwisgpf"

# Se crea el motor para conectarse a la base de datos MySQL usando SQLAlchemy
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# Carpeta donde se guardarán los archivos procesados
ruta_base = "../Trabajo_final_PBD_Terrawatt/BBDD_Preparacion_Introduccion"
ruta_salida_dim = os.path.join(ruta_base, "tablas_dim")
os.makedirs(ruta_salida_dim, exist_ok=True)

# === FUNCIONES PARA ASOCIAR IDs DE DIMENSIONES ===
# busca a qué "rango" pertenece un número (como potencia o cantidad de residentes) y devuelve el ID correspondiente.
def asignar_id_rango(valor, df_dim, col_min, col_max, col_id):
    """Asigna el ID de una dimensión basado en un rango de valores."""
    if df_dim is None or df_dim.empty:
        print(f"Error: DataFrame de dimensión está vacío o no definido.")
        return None
    for _, row in df_dim.iterrows():
        if row[col_min] <= valor < row[col_max]:
            return row[col_id]
    return None

# === DEFINICIONES DE TABLAS (definen todas las estructuras de las tablas) ===
table_definitions = {
    "Datos_precios_SQL_ID": """
        CREATE TABLE Datos_precios_SQL_ID (
            ID_precios INT PRIMARY KEY,
            ALTITUD FLOAT,
            TMEDIA FLOAT,
            TMIN FLOAT,
            TMAX FLOAT,
            DIR FLOAT,
            VELMEDIA FLOAT,
            RACHA FLOAT,
            SOL FLOAT,
            PRESMAX FLOAT,
            PRESMIN FLOAT,
            Precio_total_con_impuestos FLOAT,
            Festivo VARCHAR(10),
            Entre_semana VARCHAR(10),
            ID_Tiempo_Dia INT,
            ID_provincia INT
        )
    """,
    "Datos_consumo_SQL_ID": """
        CREATE TABLE Datos_consumo_SQL_ID (
            ID_consumo INT PRIMARY KEY,
            Consumo_energetico_kWh_m2 FLOAT,
            TMEDIA FLOAT,
            TMIN FLOAT,
            TMAX FLOAT,
            VELMEDIA FLOAT,
            SOL FLOAT,
            PRESMAX FLOAT,
            PRESMIN FLOAT,
            ID_Tiempo_Mes_Anio INT,
            ID_vivienda INT,
            ID_provincia INT,
            ID_potencia INT,
            ID_residentes INT
        )
    """,
    "Datos_predicciones_SQL_ID": """
        CREATE TABLE Datos_predicciones_SQL_ID (
            ID_predicciones INT PRIMARY KEY,
            PREDICCION_CONSUMO FLOAT,
            PREDICCION_PRECIO FLOAT,
            COSTE_POTENCIA FLOAT,
            COSTE_ESTIMADO FLOAT,
            ID_Tiempo_Dia INT,
            ID_vivienda INT,
            ID_provincia INT,
            ID_potencia INT,
            ID_residentes INT
        )
    """,
    "dim_fecha_dia": """
        CREATE TABLE dim_fecha_dia (
           ID_Tiempo_Dia INT PRIMARY KEY,
           Fecha DATE,
           AÑO INT,
           MES INT,
           DÍA INT,
           Trimestre VARCHAR(30),
           Nombre_Mes VARCHAR(30)
        )
    """,
    "dim_fecha_mes_anio": """
        CREATE TABLE dim_fecha_mes_anio (
            ID_Tiempo_Mes_Anio INT PRIMARY KEY,
            Año INT,
            Mes INT,
            Año_Mes VARCHAR(30),
            Trimestre VARCHAR(30),
            Nombre_Mes VARCHAR(30)
        )
    """,
    "dim_vivienda": """
        CREATE TABLE dim_vivienda (
            ID_vivienda INT PRIMARY KEY,
            tipo_de_vivienda VARCHAR(50)
        )
    """,
    "dim_provincia": """
        CREATE TABLE dim_provincia (
            ID_provincia INT PRIMARY KEY,
            Nombre_provincia VARCHAR(50),
            Nombre_completo VARCHAR(100)
        )
    """,
    "dim_potencia": """
        CREATE TABLE dim_potencia (
            ID_potencia INT PRIMARY KEY,
            Rango VARCHAR(50),
            Potencia_min FLOAT,
            Potencia_max FLOAT
        )
    """,
    "dim_residentes": """
        CREATE TABLE dim_residentes (
            ID_residentes INT PRIMARY KEY,
            Rango VARCHAR(50),
            Residentes_min FLOAT,
            Residentes_max FLOAT
        )
    """,
    "dim_mes": """
        CREATE TABLE dim_mes (
            ID_mes INT PRIMARY KEY,
            Nombre_mes VARCHAR(50)
        )
    """
}

# === PROCESAMIENTO DE ARCHIVO DE PRECIOS ===
print("📦 Procesando archivo de precios...")
ruta_precios = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_limpios/Modelo_Precios_Met_Fest.csv"
df_precios = pd.read_csv(ruta_precios, delimiter=';', encoding='utf-8')

# Si no existe una columna con identificador único (ID), se la creamos para cada fila.
if 'ID_precios' not in df_precios.columns:
    df_precios.insert(0, "ID_precios", range(1, len(df_precios) + 1))
# Renombramos columnas para que no tengan espacios ni símbolos especiales y sean más fáciles de manejar.
df_precios.rename(columns={
    "Precio total con impuestos (€/MWh)": "Precio_total_con_impuestos",
    "Entre semana": "Entre_semana"
}, inplace=True)

# === PROCESAMIENTO DE ARCHIVOS DE CONSUMO ===
# Buscamos todos los archivos de consumo energético dentro de la carpeta especificada y los unimos en un df.
print("\n📦 Procesando archivos de consumo...")
carpeta_consumo = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_limpios/Datos_consumo_generados_meteorologicos"
dataframes = []
for archivo in os.listdir(carpeta_consumo):
    if archivo.endswith(".csv"):
        ruta = os.path.join(carpeta_consumo, archivo)
        print(f"   ➤ Procesando: {archivo}")
        df = pd.read_csv(ruta, delimiter=',', encoding='utf-8', low_memory=False)
        df.columns = [col.strip() for col in df.columns]
        dataframes.append(df)

df_consumo = pd.concat(dataframes, ignore_index=True)
# Igual que con los precios, renombramos columnas para que tengan nombres claros y sin símbolos.
df_consumo.rename(columns={
    "Consumo energético (kWh/m²)": "Consumo_energetico_kWh_m2",
    "Media de residentes": "Media_de_residentes",
    "Potencia contratada (kW)": "Potencia_contratada_kW",
    "Tipo de vivienda": "Tipo_de_vivienda"
}, inplace=True)
# Si no hay una columna con identificador único, la creamos desde 1.
if 'ID_consumo' not in df_consumo.columns:
    df_consumo.insert(0, 'ID_consumo', range(1, len(df_consumo) + 1))

# === PROCESAMIENTO DE ARCHIVO DE PREDICCIONES WEB ===
print("\n📦 Procesando archivo de predicciones web...")
ruta_predicciones = os.path.join(ruta_base, "Datos_generados_predicciones_web.csv")
df_predicciones = pd.read_csv(ruta_predicciones, delimiter=';', encoding='utf-8')
# Si no hay una columna con identificador único, la creamos desde 1.
if 'ID_predicciones' not in df_predicciones.columns:
    df_predicciones.insert(0, 'ID_predicciones', range(1, len(df_predicciones) + 1))

# === CREACIÓN DE TABLAS DIMENSIONALES ===
# Creamos una tabla que representa cada día único que aparece en los datos, con detalles como día, mes, año, trimestre, etc.
# 1. Dimensión Fecha Diaria (dim_fecha_dia)
print("📅 Creando dimensión fecha diaria...")
fechas = []
# Intentamos convertir las fechas en formato de texto a formato fecha real. Probamos varios formatos por si hay diferencias.
if 'FECHA' in df_precios.columns:
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']:
        try:
            df_precios['FECHA'] = pd.to_datetime(df_precios['FECHA'], format=fmt, errors='coerce')
            break
        except ValueError:
            continue
#Si hay fechas válidas, las extrae (quitando nulos) y las agrega a una lista, si no saca un error
    if df_precios['FECHA'].isnull().all():
        print("⚠️ No se pudo convertir la columna 'Fecha' en df_precios a datetime. Verifica el formato.")
    else:
        fechas.extend(df_precios['FECHA'].dropna().unique())

# Intentamos convertir la columna 'FECHA_PREDICCION' a tipo fecha, probando distintos formatos comunes
if 'FECHA_PREDICCION' in df_predicciones.columns:
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']:
        try:
            df_predicciones['FECHA_PREDICCION'] = pd.to_datetime(df_predicciones['FECHA_PREDICCION'], format=fmt, errors='coerce')
            break
        except ValueError:
            continue
# Si se lograron convertir fechas en 'FECHA_PREDICCION', las agregamos a la lista 'fechas'.
    if df_predicciones['FECHA_PREDICCION'].isnull().all():
        print("⚠️ No se pudo convertir la columna 'FECHA_PREDICCION' en df_predicciones a datetime. Verifica el formato.")
    else:
        fechas.extend(df_predicciones['FECHA_PREDICCION'].dropna().unique())
        
# Si tenemos fechas válidas, encontramos la más antigua y la más reciente para crear un rango.
if fechas:
    fecha_min = min(fechas)
    fecha_max = max(fechas)
    # Creamos una lista con todos los días entre esas dos fechas, uno por fila.
    date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')
    # A cada fecha le agregamos información extra: año, mes, día, trimestre (ej: 2023Q1) y nombre del mes (Ej: Abril).
    dim_fecha_dia = pd.DataFrame({
        'Fecha': date_range,
        'Año': date_range.year,
        'Mes': date_range.month,
        'Día': date_range.day,
        'Trimestre': date_range.to_period('Q').astype(str),
        'Nombre_Mes': date_range.strftime('%B').str.capitalize()
    })
# Creamos un ID único por cada día, usando el formato DíaMesAño como número (por ejemplo: 01042023).
    dim_fecha_dia['ID_Tiempo_Dia'] = dim_fecha_dia['Fecha'].apply(lambda x: int(x.strftime('%d%m%Y')))
#Ordenamos las columnas
    dim_fecha_dia = dim_fecha_dia[['ID_Tiempo_Dia', 'Fecha', 'Año', 'Mes', 'Día', 'Trimestre', 'Nombre_Mes']]

# 2. Dimensión Fecha Mensual-Año (dim_fecha_mes_anio)
# Ahora vamos a construir una tabla que tenga una fila por cada combinación de mes y año encontrada en el consumo.
print("📅 Creando dimensión fecha mensual-año...")
if 'Fecha' in df_consumo.columns:
    # Aseguramos que la columna 'Fecha' esté en formato fecha real (datetime)
    df_consumo['Fecha'] = pd.to_datetime(df_consumo['Fecha'], errors='coerce')
    # Creamos una nueva columna con el año y mes combinados (por ejemplo: "2023-04") para poder agrupar los datos por mes.
    df_consumo['Año_Mes'] = df_consumo['Fecha'].dt.strftime('%Y-%m')
    # Obtenemos todas las combinaciones únicas de año-mes que aparecen en los datos (sin nulos).
    meses_unicos = df_consumo['Año_Mes'].dropna().unique()
    # Creamos una nueva tabla con las columnas: año-mes, año y mes.
    dim_fecha_mes_anio = pd.DataFrame({
        'Año_Mes': sorted(meses_unicos),
        'Año': [int(x.split('-')[0]) for x in sorted(meses_unicos)],
        'Mes': [int(x.split('-')[1]) for x in sorted(meses_unicos)]
    })
    # Creamos un ID único por cada año-mes con formato "MMYYYY".
    dim_fecha_mes_anio['ID_Tiempo_Mes_Anio'] = dim_fecha_mes_anio['Año_Mes'].apply(lambda x: f"{x.split('-')[1]}{x.split('-')[0]}")
    # Añadimos el trimestre al que pertenece cada mes y nombre del mes.
    dim_fecha_mes_anio['Trimestre'] = pd.to_datetime(dim_fecha_mes_anio['Año_Mes'] + '-01').dt.to_period('Q').astype(str)
    dim_fecha_mes_anio['Nombre_Mes'] = pd.to_datetime(dim_fecha_mes_anio['Año_Mes'] + '-01').dt.strftime('%B').str.capitalize()
    #Ordenar columnas
    dim_fecha_mes_anio = dim_fecha_mes_anio[['ID_Tiempo_Mes_Anio', 'Año', 'Mes', 'Año_Mes', 'Trimestre', 'Nombre_Mes']]

# 3. Dimensión Tipo de Vivienda
print("🏠 Creando dimensión tipo de vivienda...")
if 'Tipo_de_vivienda' in df_consumo.columns or 'TIPOVIVIENDA' in df_predicciones.columns:
    tipos_vivienda = set()
    # Añadimos los tipos únicos de vivienda encontrados en el archivo de consumo (ignorando valores vacíos).
    if 'Tipo_de_vivienda' in df_consumo.columns:
        tipos_vivienda.update(df_consumo['Tipo_de_vivienda'].dropna().unique())
    # Añadimos los tipos únicos de vivienda encontrados en el archivo de predicción (ignorando valores vacíos).
    if 'TIPOVIVIENDA' in df_predicciones.columns:
        tipos_vivienda.update(df_predicciones['TIPOVIVIENDA'].dropna().unique())
    dim_vivienda = pd.DataFrame({'tipo_de_vivienda': list(tipos_vivienda)})
    # Le asignamos a cada tipo un ID numérico que comienza desde 1.
    dim_vivienda.insert(0, 'ID_vivienda', range(1, len(dim_vivienda) + 1))

# 4. Dimensión Provincia
print("🌍 Creando dimensión provincia...")
if 'Provincia' in df_consumo.columns or 'PROVINCIA' in df_predicciones.columns or 'Provincia' in df_precios.columns:
    provincias = set()
    # Si los archivos tienen la columna 'Provincia', añadimos todas las provincias únicas (no nulas).
    if 'Provincia' in df_consumo.columns:
        provincias.update(df_consumo['Provincia'].dropna().unique())
    if 'PROVINCIA' in df_predicciones.columns:
        provincias.update(df_predicciones['PROVINCIA'].dropna().unique())
    if 'Provincia' in df_precios.columns:
        provincias.update(df_precios['Provincia'].dropna().unique())
    # Convertimos el conjunto de provincias en una tabla (DataFrame) con una columna: 'Nombre_provincia'.
    dim_provincia = pd.DataFrame({'Nombre_provincia': list(provincias)})
    # A cada provincia le asignamos un número identificador único (empezando desde 1).
    dim_provincia.insert(0, 'ID_provincia', range(1, len(dim_provincia) + 1))
    # Añadimos una columna con el nombre completo de la provincia + España
    dim_provincia['Nombre_completo'] = dim_provincia['Nombre_provincia'] + ", España"

# 5. Dimensión Potencia
# Vamos a crear una tabla de rangos de potencia contratada
print("⚡️ Creando dimensión potencia...")
# Calculamos el valor mínimo y máximo de potencia que aparece en los datos de consumo y predicciones.
# Así sabremos desde qué potencia hasta cuál necesitamos crear los rangos.
pot_min = min(df_consumo['Potencia_contratada_kW'].min(), df_predicciones['POTENCIA'].min())
pot_max = max(df_consumo['Potencia_contratada_kW'].max(), df_predicciones['POTENCIA'].max())
# Si alguno de los valores es nulo, mostramos un error. Si todo está bien, creamos los intervalos de potencia en este caso con saltos de 0.5 kW.
if pd.isna(pot_min) or pd.isna(pot_max):
    print("⚠️ Error: Valores nulos en Potencia_contratada_kW o POTENCIA. Verifica los datos.")
    dim_potencia = pd.DataFrame(columns=['ID_potencia', 'Rango', 'Potencia_min', 'Potencia_max'])
else:
    pot_intervals = np.arange(np.floor(pot_min * 2) / 2, np.ceil(pot_max) + 1.5, 0.5)
    dim_potencia = []
    # Recorremos los intervalos y construimos un diccionario por cada uno:
    for i in range(len(pot_intervals) - 1):
        dim_potencia.append({
            "ID_potencia": i + 1,
            "Rango": f"{pot_intervals[i]:.1f}–{pot_intervals[i+1]:.1f} kW",
            "Potencia_min": pot_intervals[i],
            "Potencia_max": pot_intervals[i+1]
        })
    # Convertimos la lista de diccionarios en una tabla (DataFrame) y mostramos cuántos rangos se generaron.
    dim_potencia = pd.DataFrame(dim_potencia)
print(f"✅ Dimensión potencia creada con {len(dim_potencia)} rangos.")

# 6. Dimensión Residentes
# Vamos a crear una tabla con rangos del número de residentes en cada hogar
print("👨‍👩‍👧 Creando dimensión residentes...")
# Calculamos el valor mínimo y máximo del número medio de personas que viven en las viviendas,
# usando tanto los datos de consumo como los de predicción.
res_min = min(df_consumo['Media_de_residentes'].min(), df_predicciones['NRESIDENTES'].min())
res_max = max(df_consumo['Media_de_residentes'].max(), df_predicciones['NRESIDENTES'].max())
# Si alguno de los valores está vacío o no es un número, lanzamos una advertencia.
# Si están bien, creamos una secuencia de valores que va desde el mínimo hasta el máximo, en pasos de 0.5.
if pd.isna(res_min) or pd.isna(res_max):
    print("⚠️ Error: Valores nulos en Media_de_residentes o NRESIDENTES. Verifica los datos.")
    dim_residentes = pd.DataFrame(columns=['ID_residentes', 'Rango', 'Residentes_min', 'Residentes_max'])
else:
    res_intervals = np.arange(np.floor(res_min * 2) / 2, np.ceil(res_max) + 1.5, 0.5)
    # Creamos una fila para cada rango de residentes, con su ID, su texto descriptivo y los valores mínimo y máximo.
    dim_residentes = []
    for i in range(len(res_intervals) - 1):
        dim_residentes.append({
            "ID_residentes": i + 1,
            "Rango": f"{res_intervals[i]:.1f}–{res_intervals[i+1]:.1f} residentes",
            "Residentes_min": res_intervals[i],
            "Residentes_max": res_intervals[i+1]
        })
    # Convertimos la lista a DataFrame y mostramos cuántos rangos se generaron en total.
    dim_residentes = pd.DataFrame(dim_residentes)
print(f"✅ Dimensión residentes creada con {len(dim_residentes)} rangos.")

# 7. Dimensión Mes
print("📅 Creando dimensión mes...")
# Creamos una tabla fija con los 12 meses del año con su id correspondiente.
meses = [
    {"ID_mes": 1, "Nombre_mes": "Enero"},
    {"ID_mes": 2, "Nombre_mes": "Febrero"},
    {"ID_mes": 3, "Nombre_mes": "Marzo"},
    {"ID_mes": 4, "Nombre_mes": "Abril"},
    {"ID_mes": 5, "Nombre_mes": "Mayo"},
    {"ID_mes": 6, "Nombre_mes": "Junio"},
    {"ID_mes": 7, "Nombre_mes": "Julio"},
    {"ID_mes": 8, "Nombre_mes": "Agosto"},
    {"ID_mes": 9, "Nombre_mes": "Septiembre"},
    {"ID_mes": 10, "Nombre_mes": "Octubre"},
    {"ID_mes": 11, "Nombre_mes": "Noviembre"},
    {"ID_mes": 12, "Nombre_mes": "Diciembre"}
]
dim_mes = pd.DataFrame(meses)

# === AÑADIR IDs DE DIMENSIONES A TABLAS PRINCIPALES ===
# 1. Tabla de Precios
print("🔗 Añadiendo IDs a la tabla de precios...")
# Vamos a añadir los IDs de fecha y provincia a la tabla de precios, para que cada fila se pueda relacionar con las dimensiones.
# Renombramos la columna de fecha para que coincida con la usada en la dimensión.
# Luego, hacemos un merge para obtener el ID correspondiente a cada fecha.
if 'FECHA' in df_precios.columns:
    df_precios.rename(columns={'FECHA': 'Fecha'}, inplace=True)
    df_precios = pd.merge(df_precios, dim_fecha_dia[['Fecha', 'ID_Tiempo_Dia']], on='Fecha', how='left')
# Igual que con la fecha, conectamos cada provincia en la tabla de precios con su ID correspondiente de la dimensión provincia.
if 'Provincia' in df_precios.columns:
    df_precios = pd.merge(df_precios, dim_provincia[['ID_provincia', 'Nombre_provincia']].rename(columns={'Nombre_provincia': 'Provincia'}), on='Provincia', how='left')

# 2. Tabla de Consumo
# Ahora hacemos lo mismo con la tabla de consumo: añadimos IDs de dimensiones para fecha, vivienda, provincia, etc.
print("🔗 Añadiendo IDs a la tabla de consumo...")
if 'Fecha' in df_consumo.columns:
    # A partir de la fecha original, sacamos el año-mes y buscamos su ID en la dimensión mensual-año.
    df_consumo['Año_Mes'] = df_consumo['Fecha'].dt.strftime('%Y-%m')
    df_consumo = pd.merge(df_consumo, dim_fecha_mes_anio[['Año_Mes', 'ID_Tiempo_Mes_Anio']], on='Año_Mes', how='left')
# Asociamos cada tipo de vivienda con su ID usando la dimensión de vivienda.
if 'Tipo_de_vivienda' in df_consumo.columns:
    df_consumo = pd.merge(df_consumo, dim_vivienda[['ID_vivienda', 'tipo_de_vivienda']].rename(columns={'tipo_de_vivienda': 'Tipo_de_vivienda'}), on='Tipo_de_vivienda', how='left')
 # Asociamos cada fila con su ID usando la dimensión de provincia.
if 'Provincia' in df_consumo.columns:
    df_consumo = pd.merge(df_consumo, dim_provincia[['ID_provincia', 'Nombre_provincia']].rename(columns={'Nombre_provincia': 'Provincia'}), on='Provincia', how='left')
# Usamos una función que compara la potencia contratada de cada fila con los rangos definidos en la dimensión potencia y devuelve el ID del rango al que pertenece.
if 'Potencia_contratada_kW' in df_consumo.columns:
    if dim_potencia is not None and not dim_potencia.empty:
        df_consumo['ID_potencia'] = df_consumo['Potencia_contratada_kW'].apply(
            lambda x: asignar_id_rango(x, dim_potencia, 'Potencia_min', 'Potencia_max', 'ID_potencia')
        )
        print("✅ Añadida columna ID_potencia a la tabla de consumo.")
    else:
        print("⚠️ Error: dim_potencia no está definido o está vacío.")
# Lo mismo con el número de residentes: buscamos el rango al que pertenece y asignamos el ID correspondiente.
if 'Media_de_residentes' in df_consumo.columns:
    if dim_residentes is not None and not dim_residentes.empty:
        df_consumo['ID_residentes'] = df_consumo['Media_de_residentes'].apply(
            lambda x: asignar_id_rango(x, dim_residentes, 'Residentes_min', 'Residentes_max', 'ID_residentes')
        )
        print("✅ Añadida columna ID_residentes a la tabla de consumo.")
    else:
        print("⚠️ Error: dim_residentes no está definido o está vacío.")

# 3. Tabla de Predicciones
print("🔗 Añadiendo IDs a la tabla de predicciones...")
# Asociamos cada predicción con su fecha diaria correspondiente (usando el ID de la dimensión de fecha), esto se repite con todas las dimensioines.
if 'FECHA_PREDICCION' in df_predicciones.columns:
    df_predicciones = pd.merge(df_predicciones, dim_fecha_dia[['Fecha', 'ID_Tiempo_Dia']],
                               left_on='FECHA_PREDICCION', right_on='Fecha', how='left')
    df_predicciones = df_predicciones.drop(columns=['Fecha'], errors='ignore')
if 'TIPOVIVIENDA' in df_predicciones.columns:
    df_predicciones = pd.merge(df_predicciones, dim_vivienda[['ID_vivienda', 'tipo_de_vivienda']].rename(columns={'tipo_de_vivienda': 'TIPOVIVIENDA'}), on='TIPOVIVIENDA', how='left')
if 'PROVINCIA' in df_predicciones.columns:
    df_predicciones = pd.merge(df_predicciones, dim_provincia[['ID_provincia', 'Nombre_provincia']].rename(columns={'Nombre_provincia': 'PROVINCIA'}), on='PROVINCIA', how='left')
if 'POTENCIA' in df_predicciones.columns:
    if dim_potencia is not None and not dim_potencia.empty:
        df_predicciones['ID_potencia'] = df_predicciones['POTENCIA'].apply(
            lambda x: asignar_id_rango(x, dim_potencia, 'Potencia_min', 'Potencia_max', 'ID_potencia')
        )
        print("✅ Añadida columna ID_potencia a la tabla de predicciones.")
    else:
        print("⚠️ Error: dim_potencia no está definido o está vacío.")
if 'NRESIDENTES' in df_predicciones.columns:
    if dim_residentes is not None and not dim_residentes.empty:
        df_predicciones['ID_residentes'] = df_predicciones['NRESIDENTES'].apply(
            lambda x: asignar_id_rango(x, dim_residentes, 'Residentes_min', 'Residentes_max', 'ID_residentes')
        )
        print("✅ Añadida columna ID_residentes a la tabla de predicciones.")
    else:
        print("⚠️ Error: dim_residentes no está definido o está vacío.")

# === ELIMINAR COLUMNAS ORIGINALES REEMPLAZADAS POR IDs ===
print("🗑️ Eliminando columnas innecesarias...")
# Quitamos las columnas 'Provincia' y 'Fecha' porque ya las reemplazamos con los IDs correspondientes
# (ID_provincia e ID_Tiempo_Dia).
df_precios = df_precios.drop(columns=['Provincia', 'Fecha'], errors='ignore')
df_consumo = df_consumo.drop(columns=['Tipo_de_vivienda', 'Provincia', 'Potencia_contratada_kW', 'Media_de_residentes', 'Fecha', 'Año_Mes'], errors='ignore')
# Eliminamos de la tabla de predicciones las columnas originales:
# tipo de vivienda, provincia, potencia, residentes, fecha de predicción y mes textual.
# Ya tenemos todo eso representado con IDs en las columnas correspondientes.
df_predicciones = df_predicciones.drop(columns=['TIPOVIVIENDA', 'PROVINCIA', 'POTENCIA', 'NRESIDENTES', 'FECHA_PREDICCION', 'MES'], errors='ignore')

# === CREAR TABLAS EN MySQL ===
# Nos conectamos a la base de datos y eliminamos las tablas si ya existen.
print("🛠️ Creando tablas en MySQL...")
with engine.connect() as connection:
    for table_name in table_definitions.keys():
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        print(f"🗑️ Tabla '{table_name}' eliminada si existía.")
    # Creamos nuevamente todas las tablas, usando las definiciones SQL que preparamos al principio del código.
    for table_name, definition in table_definitions.items():
        connection.execute(text(definition))
        print(f"✅ Tabla '{table_name}' creada en MySQL.")

# === SUBIR DATOS A MySQL ===
print("📤 Subiendo datos a MySQL...")
# Dimensional tables
# Cargamos la tabla de fechas diarias a MySQL. Repetimos lo mismo para las demás dimensiones.
dim_fecha_dia.to_sql('dim_fecha_dia', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_fecha_dia' actualizada con {len(dim_fecha_dia)} registros.")
dim_fecha_mes_anio.to_sql('dim_fecha_mes_anio', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_fecha_mes_anio' actualizada con {len(dim_fecha_mes_anio)} registros.")
dim_vivienda.to_sql('dim_vivienda', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_vivienda' actualizada con {len(dim_vivienda)} registros.")
dim_provincia.to_sql('dim_provincia', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_provincia' actualizada con {len(dim_provincia)} registros.")
dim_potencia.to_sql('dim_potencia', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_potencia' actualizada con {len(dim_potencia)} registros.")
dim_residentes.to_sql('dim_residentes', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_residentes' actualizada con {len(dim_residentes)} registros.")
dim_mes.to_sql('dim_mes', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'dim_mes' actualizada con {len(dim_mes)} registros.")

# Main tables
# Ahora subimos la tabla de precios ya transformada con IDs.
# Hacemos lo mismo con las tablas de consumo y predicciones.
df_precios.to_sql('Datos_precios_SQL_ID', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'Datos_precios_SQL_ID' actualizada con {len(df_precios)} registros.")
df_consumo.to_sql('Datos_consumo_SQL_ID', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'Datos_consumo_SQL_ID' actualizada con {len(df_consumo)} registros.")
df_predicciones.to_sql('Datos_predicciones_SQL_ID', con=engine, if_exists='append', index=False)
print(f"✅ Tabla 'Datos_predicciones_SQL_ID' actualizada con {len(df_predicciones)} registros.")

# === DIAGNÓSTICO DE VALORES NULOS ===
# Revisamos si quedaron valores nulos en alguna de las tablas.
print("\n🔍 Diagnóstico de valores nulos...")
print("Precios:", df_precios.isnull().sum().sum())
print("Consumo:", df_consumo.isnull().sum().sum())
print("Predicciones:", df_predicciones.isnull().sum().sum()) 

# === GUARDAR ARCHIVOS CSV (COMENTADO) ===
## Guarda las tablas final de precios, consumos y predicciones (con IDs incluidos) en un archivo CSV
## Tambien guardamos cada dimensión en un archivo CSV en la carpeta "tablas_dim", una por una.
"""
output_precios = os.path.join(ruta_base, "Datos_precios_SQL_ID.csv")
df_precios.to_csv(output_precios, sep=';', index=False, encoding='utf-8')
print(f"✅ Archivo de precios guardado en: {output_precios}")

output_consumo = os.path.join(ruta_base, "Datos_consumo_SQL_ID.csv")
df_consumo.to_csv(output_consumo, sep=';', index=False, encoding='utf-8')
print(f"✅ Archivo de consumo guardado en: {output_consumo}")

output_predicciones = os.path.join(ruta_base, "Datos_predicciones_SQL_ID.csv")
df_predicciones.to_csv(output_predicciones, sep=';', index=False, encoding='utf-8')
print(f"✅ Archivo de predicciones guardado en: {output_predicciones}")



dim_fecha_dia.to_csv(os.path.join(ruta_salida_dim, "dim_fecha_dia.csv"), sep=';', index=False, encoding='utf-8')
dim_fecha_mes_anio.to_csv(os.path.join(ruta_salida_dim, "dim_fecha_mes_anio.csv"), sep=';', index=False, encoding='utf-8')
dim_vivienda.to_csv(os.path.join(ruta_salida_dim, "dim_vivienda.csv"), sep=';', index=False, encoding='utf-8')
dim_provincia.to_csv(os.path.join(ruta_salida_dim, "dim_provincia.csv"), sep=';', index=False, encoding='utf-8')
dim_potencia.to_csv(os.path.join(ruta_salida_dim, "dim_potencia.csv"), sep=';', index=False, encoding='utf-8')
dim_residentes.to_csv(os.path.join(ruta_salida_dim, "dim_residentes.csv"), sep=';', index=False, encoding='utf-8')
dim_mes.to_csv(os.path.join(ruta_salida_dim, "dim_mes.csv"), sep=';', index=False, encoding='utf-8')
"""