import os
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

# Credenciales de acceso a la base de datos
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
def asignar_id_rango(valor, df_dim, col_min, col_max, col_id):
    """Asigna el ID de una dimensión basado en un rango de valores."""
    if df_dim is None or df_dim.empty:
        print(f"Error: DataFrame de dimensión está vacío o no definido.")
        return None
    for _, row in df_dim.iterrows():
        if row[col_min] <= valor < row[col_max]:
            return row[col_id]
    return None

# === DEFINICIONES DE TABLAS ===
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
    """,
    "dim_festivo": """
        CREATE TABLE dim_festivo (
            ID_festivo INT PRIMARY KEY,
            Festividad VARCHAR(100)
        )
    """,
    "tabla_puente_fecha_provincia_festivo": """
        CREATE TABLE tabla_puente_fecha_provincia_festivo (
            ID_Tiempo_Dia INT,
            ID_provincia INT,
            ID_festivo INT,
            Es_Festivo VARCHAR(3),
            PRIMARY KEY (ID_Tiempo_Dia, ID_provincia),
            FOREIGN KEY (ID_Tiempo_Dia) REFERENCES dim_fecha_dia(ID_Tiempo_Dia),
            FOREIGN KEY (ID_provincia) REFERENCES dim_provincia(ID_provincia),
            FOREIGN KEY (ID_festivo) REFERENCES dim_festivo(ID_festivo)
        )
    """
}

# === PROCESAMIENTO DE ARCHIVO DE PRECIOS ===
ruta_precios = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_limpios/Modelo_Precios_Met_Fest.csv"
df_precios = pd.read_csv(ruta_precios, delimiter=';', encoding='utf-8')

if 'ID_precios' not in df_precios.columns:
    df_precios.insert(0, "ID_precios", range(1, len(df_precios) + 1))
df_precios.rename(columns={
    "Precio total con impuestos (€/MWh)": "Precio_total_con_impuestos",
    "Entre semana": "Entre_semana"
}, inplace=True)

# === PROCESAMIENTO DE ARCHIVOS DE CONSUMO ===
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
df_consumo.rename(columns={
    "Consumo energético (kWh/m²)": "Consumo_energetico_kWh_m2",
    "Media de residentes": "Media_de_residentes",
    "Potencia contratada (kW)": "Potencia_contratada_kW",
    "Tipo de vivienda": "Tipo_de_vivienda"
}, inplace=True)
if 'ID_consumo' not in df_consumo.columns:
    df_consumo.insert(0, 'ID_consumo', range(1, len(df_consumo) + 1))

# === PROCESAMIENTO DE ARCHIVO DE PREDICCIONES WEB ===
ruta_predicciones = os.path.join(ruta_base, "Datos_generados_predicciones_web.csv")
df_predicciones = pd.read_csv(ruta_predicciones, delimiter=';', encoding='utf-8')
if 'ID_predicciones' not in df_predicciones.columns:
    df_predicciones.insert(0, 'ID_predicciones', range(1, len(df_predicciones) + 1))

# === CREACIÓN DE TABLAS DIMENSIONALES ===
# 1. Dimensión Fecha Diaria (dim_fecha_dia)
fechas = []
if 'FECHA' in df_precios.columns:
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']:
        try:
            df_precios['FECHA'] = pd.to_datetime(df_precios['FECHA'], format=fmt, errors='coerce')
            break
        except ValueError:
            continue
    if df_precios['FECHA'].isnull().all():
        print("No se pudo convertir la columna 'Fecha' en df_precios a datetime. Verifica el formato.")
    else:
        fechas.extend(df_precios['FECHA'].dropna().unique())

if 'FECHA_PREDICCION' in df_predicciones.columns:
    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']:
        try:
            df_predicciones['FECHA_PREDICCION'] = pd.to_datetime(df_predicciones['FECHA_PREDICCION'], format=fmt, errors='coerce')
            break
        except ValueError:
            continue
    if df_predicciones['FECHA_PREDICCION'].isnull().all():
        print("No se pudo convertir la columna 'FECHA_PREDICCION' en df_predicciones a datetime. Verifica el formato.")
    else:
        fechas.extend(df_predicciones['FECHA_PREDICCION'].dropna().unique())

# 8. Dimensión Festivo (cargamos Festivos.csv para incluir sus fechas en dim_fecha_dia)
ruta_festivos = "../Trabajo_final_PBD_Terrawatt/Datos_Y_Limpieza/Datos_brutos/Festivos.csv"
df_festivos = pd.read_csv(ruta_festivos, delimiter=';', encoding='utf-8')

# Convertir fechas de Festivos.csv para incluirlas en el rango
for fmt in ['%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
    df_festivos['Fecha'] = pd.to_datetime(df_festivos['Fecha'], format=fmt, errors='coerce')
    if not df_festivos['Fecha'].isnull().all():
        break
if df_festivos['Fecha'].isnull().any():
    print("Advertencia: Algunas fechas en df_festivos no se pudieron convertir:")
    print(df_festivos[df_festivos['Fecha'].isnull()][['Fecha', 'Provincia', 'Festividad']])
else:
    fechas.extend(df_festivos['Fecha'].dropna().unique())

# Crear dim_fecha_dia con el rango completo de fechas
if fechas:
    fecha_min = min(fechas)
    fecha_max = max(fechas)
    date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')
    dim_fecha_dia = pd.DataFrame({
        'Fecha': date_range,
        'Año': date_range.year,
        'Mes': date_range.month,
        'Día': date_range.day,
        'Trimestre': date_range.to_period('Q').astype(str),
        'Nombre_Mes': date_range.strftime('%B').str.capitalize()
    })
    dim_fecha_dia['ID_Tiempo_Dia'] = dim_fecha_dia['Fecha'].apply(lambda x: int(x.strftime('%d%m%Y')))
    dim_fecha_dia = dim_fecha_dia[['ID_Tiempo_Dia', 'Fecha', 'Año', 'Mes', 'Día', 'Trimestre', 'Nombre_Mes']]
    print("Rango de fechas en dim_fecha_dia:", dim_fecha_dia['Fecha'].min(), "a", dim_fecha_dia['Fecha'].max())

# 2. Dimensión Fecha Mensual-Año (dim_fecha_mes_anio)
if 'Fecha' in df_consumo.columns:
    df_consumo['Fecha'] = pd.to_datetime(df_consumo['Fecha'], errors='coerce')
    df_consumo['Año_Mes'] = df_consumo['Fecha'].dt.strftime('%Y-%m')
    meses_unicos = df_consumo['Año_Mes'].dropna().unique()
    dim_fecha_mes_anio = pd.DataFrame({
        'Año_Mes': sorted(meses_unicos),
        'Año': [int(x.split('-')[0]) for x in sorted(meses_unicos)],
        'Mes': [int(x.split('-')[1]) for x in sorted(meses_unicos)]
    })
    dim_fecha_mes_anio['ID_Tiempo_Mes_Anio'] = dim_fecha_mes_anio['Año_Mes'].apply(lambda x: f"{x.split('-')[1]}{x.split('-')[0]}")
    dim_fecha_mes_anio['Trimestre'] = pd.to_datetime(dim_fecha_mes_anio['Año_Mes'] + '-01').dt.to_period('Q').astype(str)
    dim_fecha_mes_anio['Nombre_Mes'] = pd.to_datetime(dim_fecha_mes_anio['Año_Mes'] + '-01').dt.strftime('%B').str.capitalize()
    dim_fecha_mes_anio = dim_fecha_mes_anio[['ID_Tiempo_Mes_Anio', 'Año', 'Mes', 'Año_Mes', 'Trimestre', 'Nombre_Mes']]

# 3. Dimensión Tipo de Vivienda
if 'Tipo_de_vivienda' in df_consumo.columns or 'TIPOVIVIENDA' in df_predicciones.columns:
    tipos_vivienda = set()
    if 'Tipo_de_vivienda' in df_consumo.columns:
        tipos_vivienda.update(df_consumo['Tipo_de_vivienda'].dropna().unique())
    if 'TIPOVIVIENDA' in df_predicciones.columns:
        tipos_vivienda.update(df_predicciones['TIPOVIVIENDA'].dropna().unique())
    dim_vivienda = pd.DataFrame({'tipo_de_vivienda': list(tipos_vivienda)})
    dim_vivienda.insert(0, 'ID_vivienda', range(1, len(dim_vivienda) + 1))

# 4. Dimensión Provincia
if 'Provincia' in df_consumo.columns or 'PROVINCIA' in df_predicciones.columns or 'Provincia' in df_precios.columns:
    provincias = set()
    if 'Provincia' in df_consumo.columns:
        provincias.update(df_consumo['Provincia'].dropna().unique())
    if 'PROVINCIA' in df_predicciones.columns:
        provincias.update(df_predicciones['PROVINCIA'].dropna().unique())
    if 'Provincia' in df_precios.columns:
        provincias.update(df_precios['Provincia'].dropna().unique())
    # Añadir provincias de Festivos.csv
    if 'Provincia' in df_festivos.columns:
        provincias.update(df_festivos['Provincia'].dropna().unique())
    dim_provincia = pd.DataFrame({'Nombre_provincia': list(provincias)})
    dim_provincia.insert(0, 'ID_provincia', range(1, len(dim_provincia) + 1))
    dim_provincia['Nombre_completo'] = dim_provincia['Nombre_provincia'] + ", España"
    # Estandarizar nombres de provincias
    dim_provincia['Nombre_provincia'] = dim_provincia['Nombre_provincia'].str.strip().str.title()

# 5. Dimensión Potencia
pot_min = min(df_consumo['Potencia_contratada_kW'].min(), df_predicciones['POTENCIA'].min())
pot_max = max(df_consumo['Potencia_contratada_kW'].max(), df_predicciones['POTENCIA'].max())
if pd.isna(pot_min) or pd.isna(pot_max):
    print("Error: Valores nulos en Potencia_contratada_kW o POTENCIA. Verifica los datos.")
    dim_potencia = pd.DataFrame(columns=['ID_potencia', 'Rango', 'Potencia_min', 'Potencia_max'])
else:
    pot_intervals = np.arange(np.floor(pot_min * 2) / 2, np.ceil(pot_max) + 1.5, 0.5)
    dim_potencia = []
    for i in range(len(pot_intervals) - 1):
        dim_potencia.append({
            "ID_potencia": i + 1,
            "Rango": f"{pot_intervals[i]:.1f}–{pot_intervals[i+1]:.1f} kW",
            "Potencia_min": pot_intervals[i],
            "Potencia_max": pot_intervals[i+1]
        })
    dim_potencia = pd.DataFrame(dim_potencia)

# 6. Dimensión Residentes
res_min = min(df_consumo['Media_de_residentes'].min(), df_predicciones['NRESIDENTES'].min())
res_max = max(df_consumo['Media_de_residentes'].max(), df_predicciones['NRESIDENTES'].max())
if pd.isna(res_min) or pd.isna(res_max):
    print("Error: Valores nulos en Media_de_residentes o NRESIDENTES. Verifica los datos.")
    dim_residentes = pd.DataFrame(columns=['ID_residentes', 'Rango', 'Residentes_min', 'Residentes_max'])
else:
    res_intervals = np.arange(np.floor(res_min * 2) / 2, np.ceil(res_max) + 1.5, 0.5)
    dim_residentes = []
    for i in range(len(res_intervals) - 1):
        dim_residentes.append({
            "ID_residentes": i + 1,
            "Rango": f"{res_intervals[i]:.1f}–{res_intervals[i+1]:.1f} residentes",
            "Residentes_min": res_intervals[i],
            "Residentes_max": res_intervals[i+1]
        })
    dim_residentes = pd.DataFrame(dim_residentes)

# 7. Dimensión Mes
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

# 8. Dimensión Festivo (ya cargado df_festivos)
# Depuración: Verificar datos iniciales
print("Primeras filas de df_festivos:\n", df_festivos.head())
print("Formato de Fecha en df_festivos:\n", df_festivos['Fecha'].head())
print("Provincias en df_festivos:\n", df_festivos['Provincia'].unique())
print("Rango de fechas en df_festivos:", df_festivos['Fecha'].min(), "a", df_festivos['Fecha'].max())

# Eliminar duplicados
df_festivos = df_festivos.drop_duplicates(subset=['Fecha', 'Provincia'], keep='first')

# Estandarizar nombres de provincias
df_festivos['Provincia'] = df_festivos['Provincia'].str.strip().str.title()

# Crear dim_festivo
festividades_unicas = df_festivos['Festividad'].dropna().unique()
dim_festivo = pd.DataFrame({"Festividad": festividades_unicas})
dim_festivo.insert(0, 'ID_festivo', range(1, len(dim_festivo) + 1))
print("Festividades únicas en dim_festivo:\n", dim_festivo)

# 9. Tabla Puente
# Crear todas las combinaciones de ID_Tiempo_Dia y ID_provincia
all_dates = dim_fecha_dia[['ID_Tiempo_Dia']]
all_provinces = dim_provincia[['ID_provincia']]
all_combinations = all_dates.assign(key=1).merge(all_provinces.assign(key=1), on='key').drop('key', axis=1)

# Unir con dim_fecha_dia para obtener fechas
df_festivos = pd.merge(df_festivos, dim_fecha_dia[['Fecha', 'ID_Tiempo_Dia']], on='Fecha', how='left')
print("Después de unir con dim_fecha_dia:\n", df_festivos[['Fecha', 'ID_Tiempo_Dia', 'Provincia', 'Festividad']].head())
print("Filas con ID_Tiempo_Dia nulo:\n", df_festivos[df_festivos['ID_Tiempo_Dia'].isnull()][['Fecha', 'Provincia', 'Festividad']])

# Unir con dim_provincia para obtener ID_provincia
print("Provincias en dim_provincia:\n", dim_provincia['Nombre_provincia'].unique())
df_festivos = pd.merge(df_festivos, dim_provincia[['Nombre_provincia', 'ID_provincia']], 
                       left_on='Provincia', right_on='Nombre_provincia', how='left')
print("Después de unir con dim_provincia:\n", df_festivos[['Fecha', 'ID_Tiempo_Dia', 'Provincia', 'Nombre_provincia', 'ID_provincia']].head())
print("Filas con ID_provincia nulo:\n", df_festivos[df_festivos['ID_provincia'].isnull()][['Fecha', 'Provincia', 'Festividad']])

# Unir con dim_festivo para obtener ID_festivo
df_festivos = pd.merge(df_festivos, dim_festivo, left_on='Festividad', right_on='Festividad', how='left')
print("Después de unir con dim_festivo:\n", df_festivos[['Fecha', 'ID_Tiempo_Dia', 'ID_provincia', 'Festividad', 'ID_festivo']].head())
print("Filas con ID_festivo nulo:\n", df_festivos[df_festivos['ID_festivo'].isnull()][['Fecha', 'ID_Tiempo_Dia', 'Provincia', 'Festividad']])

# Crear tabla_puente_fecha_provincia_festivo con todas las combinaciones
tabla_puente_fecha_provincia_festivo = pd.merge(
    all_combinations,
    df_festivos[['ID_Tiempo_Dia', 'ID_provincia', 'ID_festivo']],
    on=['ID_Tiempo_Dia', 'ID_provincia'],
    how='left'
)

# Asignar Es_Festivo: "Sí" si hay un ID_festivo, "No" si no
tabla_puente_fecha_provincia_festivo['Es_Festivo'] = tabla_puente_fecha_provincia_festivo['ID_festivo'].notnull().map({True: 'Sí', False: 'No'})

# Eliminar duplicados y asegurar que no haya nulos en las claves primarias
tabla_puente_fecha_provincia_festivo = tabla_puente_fecha_provincia_festivo[['ID_Tiempo_Dia', 'ID_provincia', 'ID_festivo', 'Es_Festivo']]
tabla_puente_fecha_provincia_festivo = tabla_puente_fecha_provincia_festivo.drop_duplicates(subset=['ID_Tiempo_Dia', 'ID_provincia'], keep='first')
tabla_puente_fecha_provincia_festivo = tabla_puente_fecha_provincia_festivo.dropna(subset=['ID_Tiempo_Dia', 'ID_provincia'])

# Verificar integridad
expected_combinations = len(dim_fecha_dia) * len(dim_provincia)
actual_combinations = len(tabla_puente_fecha_provincia_festivo)
if actual_combinations < expected_combinations:
    print(f"Advertencia: Faltan {expected_combinations - actual_combinations} combinaciones en tabla_puente_fecha_provincia_festivo.")
print("Tamaño de tabla_puente_fecha_provincia_festivo:", len(tabla_puente_fecha_provincia_festivo))
print("Primeras filas:\n", tabla_puente_fecha_provincia_festivo.head())
print("Conteo de Es_Festivo:\n", tabla_puente_fecha_provincia_festivo['Es_Festivo'].value_counts())

# === AÑADIR IDs DE DIMENSIONES A TABLAS PRINCIPALES ===
# 1. Tabla de Precios
if 'FECHA' in df_precios.columns:
    df_precios.rename(columns={'FECHA': 'Fecha'}, inplace=True)
    df_precios = pd.merge(df_precios, dim_fecha_dia[['Fecha', 'ID_Tiempo_Dia']], on='Fecha', how='left')
if 'Provincia' in df_precios.columns:
    df_precios = pd.merge(df_precios, dim_provincia[['ID_provincia', 'Nombre_provincia']].rename(columns={'Nombre_provincia': 'Provincia'}), on='Provincia', how='left')
df_precios = df_precios.drop(columns=['Festivo', 'Provincia', 'Fecha'], errors='ignore')

# 2. Tabla de Consumo
if 'Fecha' in df_consumo.columns:
    df_consumo['Fecha'] = pd.to_datetime(df_consumo['Fecha'], errors='coerce')
    df_consumo['Año_Mes'] = df_consumo['Fecha'].dt.strftime('%Y-%m')
    df_consumo = pd.merge(df_consumo, dim_fecha_mes_anio[['Año_Mes', 'ID_Tiempo_Mes_Anio']], on='Año_Mes', how='left')
if 'Tipo_de_vivienda' in df_consumo.columns:
    df_consumo = pd.merge(df_consumo, dim_vivienda[['ID_vivienda', 'tipo_de_vivienda']].rename(columns={'tipo_de_vivienda': 'Tipo_de_vivienda'}), on='Tipo_de_vivienda', how='left')
if 'Provincia' in df_consumo.columns:
    df_consumo = pd.merge(df_consumo, dim_provincia[['ID_provincia', 'Nombre_provincia']].rename(columns={'Nombre_provincia': 'Provincia'}), on='Provincia', how='left')
if 'Potencia_contratada_kW' in df_consumo.columns:
    if dim_potencia is not None and not dim_potencia.empty:
        df_consumo['ID_potencia'] = df_consumo['Potencia_contratada_kW'].apply(
            lambda x: asignar_id_rango(x, dim_potencia, 'Potencia_min', 'Potencia_max', 'ID_potencia')
        )
        print("Añadida columna ID_potencia a la tabla de consumo.")
    else:
        print("Error: dim_potencia no está definido o está vacío.")
if 'Media_de_residentes' in df_consumo.columns:
    if dim_residentes is not None and not dim_residentes.empty:
        df_consumo['ID_residentes'] = df_consumo['Media_de_residentes'].apply(
            lambda x: asignar_id_rango(x, dim_residentes, 'Residentes_min', 'Residentes_max', 'ID_residentes')
        )
        print("Añadida columna ID_residentes a la tabla de consumo.")
    else:
        print("Error: dim_residentes no está definido o está vacío.")

# 3. Tabla de Predicciones
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
    else:
        print("Error: dim_potencia no está definido o está vacío.")
if 'NRESIDENTES' in df_predicciones.columns:
    if dim_residentes is not None and not dim_residentes.empty:
        df_predicciones['ID_residentes'] = df_predicciones['NRESIDENTES'].apply(
            lambda x: asignar_id_rango(x, dim_residentes, 'Residentes_min', 'Residentes_max', 'ID_residentes')
        )
        print("Añadida columna ID_residentes a la tabla de predicciones.")
    else:
        print("Error: dim_residentes no está definido o está vacío.")

# === ELIMINAR COLUMNAS ORIGINALES REEMPLAZADAS POR IDs ===
df_precios = df_precios.drop(columns=['Provincia', 'Fecha'], errors='ignore')
df_consumo = df_consumo.drop(columns=['Tipo_de_vivienda', 'Provincia', 'Potencia_contratada_kW', 'Media_de_residentes', 'Fecha', 'Año_Mes'], errors='ignore')
df_predicciones = df_predicciones.drop(columns=['TIPOVIVIENDA', 'PROVINCIA', 'POTENCIA', 'NRESIDENTES', 'FECHA_PREDICCION', 'MES'], errors='ignore')

# === CREAR TABLAS EN MySQL ===
with engine.connect() as connection:
    # Primero eliminamos las tablas con claves foráneas
    for table_name in ["tabla_puente_fecha_provincia_festivo", "Datos_predicciones_SQL_ID", "Datos_consumo_SQL_ID", "Datos_precios_SQL_ID"]:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        print(f" Tabla '{table_name}' eliminada si existía.")
    
    # Luego eliminamos las tablas dimensionales
    for table_name in ["dim_festivo", "dim_fecha_dia", "dim_fecha_mes_anio", "dim_vivienda", "dim_provincia", "dim_potencia", "dim_residentes", "dim_mes"]:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        print(f" Tabla '{table_name}' eliminada si existía.")
    
    # Creamos todas las tablas
    for table_name, definition in table_definitions.items():
        connection.execute(text(definition))
        print(f"Tabla '{table_name}' creada en MySQL.")

# === SUBIR DATOS A MySQL ===
print(" Subiendo datos a MySQL...")
dim_fecha_dia.to_sql('dim_fecha_dia', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_fecha_dia' actualizada con {len(dim_fecha_dia)} registros.")
dim_fecha_mes_anio.to_sql('dim_fecha_mes_anio', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_fecha_mes_anio' actualizada con {len(dim_fecha_mes_anio)} registros.")
dim_vivienda.to_sql('dim_vivienda', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_vivienda' actualizada con {len(dim_vivienda)} registros.")
dim_provincia.to_sql('dim_provincia', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_provincia' actualizada con {len(dim_provincia)} registros.")
dim_potencia.to_sql('dim_potencia', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_potencia' actualizada con {len(dim_potencia)} registros.")
dim_residentes.to_sql('dim_residentes', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_residentes' actualizada con {len(dim_residentes)} registros.")
dim_mes.to_sql('dim_mes', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_mes' actualizada con {len(dim_mes)} registros.")
dim_festivo.to_sql('dim_festivo', con=engine, if_exists='append', index=False)
print(f"Tabla 'dim_festivo' actualizada con {len(dim_festivo)} registros.")
tabla_puente_fecha_provincia_festivo.to_sql('tabla_puente_fecha_provincia_festivo', con=engine, if_exists='append', index=False)
print(f"Tabla 'tabla_puente_fecha_provincia_festivo' actualizada con {len(tabla_puente_fecha_provincia_festivo)} registros.")

df_precios.to_sql('Datos_precios_SQL_ID', con=engine, if_exists='replace', index=False)
print(f"Tabla 'Datos_precios_SQL_ID' actualizada con {len(df_precios)} registros.")
df_consumo.to_sql('Datos_consumo_SQL_ID', con=engine, if_exists='append', index=False)
print(f"Tabla 'Datos_consumo_SQL_ID' actualizada con {len(df_consumo)} registros.")
df_predicciones.to_sql('Datos_predicciones_SQL_ID', con=engine, if_exists='append', index=False)
print(f"Tabla 'Datos_predicciones_SQL_ID' actualizada con {len(df_predicciones)} registros.")