from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from pydantic import BaseModel
import os
import pandas as pd
import joblib
from sklearn.preprocessing import StandardScaler
import calendar
from datetime import datetime
import numpy as np
import mysql.connector
import unicodedata

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUTA_METEOROLOGICA = os.path.join(BASE_DIR, "..", "..", "Datos_Y_Limpieza", "Datos_limpios", "Datos_limpios_metereologicos")
RUTA_MODELOS = os.path.join(BASE_DIR, "..", "..", "Modelos", "Modelos_generados", "Modelos_consumo_por_provincia")
RUTA_MODELO_PRECIOS = os.path.join(BASE_DIR, "..", "..", "Modelos", "Modelos_generados")
modelo_precios_path = os.path.join(BASE_DIR, "..", "..", "Datos_Y_Limpieza", "Datos_limpios", "Modelo_Precios_Met_Fest.csv")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Datos(BaseModel):
    potencia: float
    numero_residentes: float
    tipo_vivienda: str
    provincia: str
    mes: int

db_config = {
    "user": "uhmzmxoizkatmdsu",
    "password": "hcG4aHLWkwV4KrjM9re",
    "host": "hv-par8-022.clvrcld.net",
    "port": "10532",
    "database": "brqtr1tzuvatzxwisgpf"
}

def normalizar_texto(texto):
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    return texto.upper()

def obtener_id_dimension(cursor, tabla, columna, valor, columna_id, es_rango=False):
    try:
        if es_rango:
            consulta = f"SELECT {columna_id} FROM {tabla} WHERE {columna}_min <= %s AND {columna}_max > %s"
            cursor.execute(consulta, (valor, valor))
        else:
            if tabla == "dim_vivienda":
                valor = valor.replace("-", " ")
            if tabla == "dim_provincia":
                valor_normalizado = normalizar_texto(valor)
                consulta = f"SELECT {columna_id} FROM {tabla} WHERE UPPER({columna}) = %s"
                cursor.execute(consulta, (valor_normalizado,))
            else:
                consulta = f"SELECT {columna_id} FROM {tabla} WHERE {columna} = %s"
                cursor.execute(consulta, (valor,))
        resultado = cursor.fetchone()
        while cursor.fetchone() is not None:
            pass
        if resultado:
            return resultado[0]
        else:
            raise ValueError(f"No se encontró un registro en {tabla} para el valor {valor} en la columna {columna}")
    except mysql.connector.Error as error:
        while cursor.fetchone() is not None:
            pass
        raise ValueError(f"Error en la consulta a {tabla}: {str(error)}")

def obtener_o_insertar_fecha(cursor):
    hoy = datetime.today()
    dia = hoy.day
    mes = hoy.month
    ano = hoy.year
    fecha = hoy.strftime("%Y-%m-%d")
    consulta = "SELECT ID_Tiempo_Dia FROM dim_fecha_dia WHERE Fecha = %s"
    cursor.execute(consulta, (fecha,))
    resultado = cursor.fetchone()
    if resultado:
        return resultado[0]
    id_fecha = int(f"{dia:02d}{mes:02d}{ano}")
    trimestre = f"{ano}Q{(mes - 1) // 3 + 1}"
    nombre_mes = calendar.month_name[mes]
    consulta_insertar = """
    INSERT INTO dim_fecha_dia (ID_Tiempo_Dia, Fecha, Año, MES, DÍA, Trimestre, Nombre_Mes)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(consulta_insertar, (id_fecha, fecha, ano, mes, dia, trimestre, nombre_mes))
    return id_fecha

@app.post("/transformar")
async def transformar_datos(datos: Datos):
    tipos_vivienda = [
        'Tipo de vivienda_Adosado',
        'Tipo de vivienda_Casa Unifamiliar',
        'Tipo de vivienda_Duplex',
        'Tipo de vivienda_Piso'
    ]
    variables_vivienda = {tipo: False for tipo in tipos_vivienda}
    if datos.tipo_vivienda == "Adosado":
        variables_vivienda['Tipo de vivienda_Adosado'] = True
    elif datos.tipo_vivienda == "Casa-unifamiliar":
        variables_vivienda['Tipo de vivienda_Casa Unifamiliar'] = True
    elif datos.tipo_vivienda == "Duplex":
        variables_vivienda['Tipo de vivienda_Duplex'] = True
    elif datos.tipo_vivienda == "Piso":
        variables_vivienda['Tipo de vivienda_Piso'] = True
    else:
        return {"error": "Tipo de vivienda no reconocido."}

    provincia = datos.provincia
    archivo_provincia = os.path.join(RUTA_METEOROLOGICA, f"{provincia}.csv")
    if not os.path.exists(archivo_provincia):
        return {"error": f"No se encontró el archivo de la provincia: {provincia}"}

    df_meteorologico = pd.read_csv(archivo_provincia, delimiter=";")
    df_meteorologico["MES"] = pd.to_datetime(df_meteorologico["FECHA"], errors='coerce').dt.month
    df_filtrado = df_meteorologico[df_meteorologico["MES"] == datos.mes]
    columnas_meteorologicas = ["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN"]
    medias_meteorologicas = df_filtrado[columnas_meteorologicas].mean()
    medias_dict = medias_meteorologicas.to_dict()

    datos_transformados = {
        "potencia": datos.potencia,
        "numero_residentes": datos.numero_residentes,
        "provincia": datos.provincia,
        "mes": datos.mes,
    }
    datos_transformados = {**datos_transformados, **medias_dict, **variables_vivienda}

    ruta_modelo = os.path.join(RUTA_MODELOS, f"Modelo_{provincia}.pkl")
    if not os.path.exists(ruta_modelo):
        return {"error": f"No se encontró el modelo para la provincia: {provincia}"}

    modelo_consumo = joblib.load(ruta_modelo)

    nombres_columnas = [
        "TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN",
        "Potencia contratada (kW)", "Mes", "Media de residentes",
        "Tipo de vivienda_Adosado", "Tipo de vivienda_Casa Unifamiliar", 
        "Tipo de vivienda_Duplex", "Tipo de vivienda_Piso"
    ]
    valores = [
        datos_transformados["TMEDIA"],
        datos_transformados["TMIN"],
        datos_transformados["TMAX"],
        datos_transformados["VELMEDIA"],
        datos_transformados["SOL"],
        datos_transformados["PRESMAX"],
        datos_transformados["PRESMIN"],
        datos.potencia,
        datos.mes,
        datos.numero_residentes,
        datos_transformados["Tipo de vivienda_Adosado"],
        datos_transformados["Tipo de vivienda_Casa Unifamiliar"],
        datos_transformados["Tipo de vivienda_Duplex"],
        datos_transformados["Tipo de vivienda_Piso"]
    ]
    df_caracteristicas = pd.DataFrame([valores], columns=nombres_columnas)
    escalador = StandardScaler()
    columnas_normalizar = ["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"]
    df_caracteristicas[columnas_normalizar] = escalador.fit_transform(df_caracteristicas[columnas_normalizar])

    prediccion_consumo = modelo_consumo.predict(df_caracteristicas)
    prediccion_consumo = max(0, prediccion_consumo[0])
    datos_transformados["prediccion_consumo"] = prediccion_consumo

    ruta_modelo_precios = os.path.join(RUTA_MODELO_PRECIOS, "Modelo_precios_mlp.pkl")

    if os.path.exists(ruta_modelo_precios):
        modelo_precios = joblib.load(ruta_modelo_precios)
        ano = 2025
        fecha_inicio = datetime(ano, datos.mes, 1)
        ultimo_dia = calendar.monthrange(ano, datos.mes)[1]
        fecha_fin = datetime(ano, datos.mes, ultimo_dia)
        rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')

        ruta_archivo_precios = os.path.join(BASE_DIR, "..", "..", "Datos_Y_Limpieza", "Datos_limpios", "Modelo_Precios_Met_Fest.csv")

        try:
            datos_precios = pd.read_csv(ruta_archivo_precios, delimiter=';')
            datos_precios['FECHA'] = pd.to_datetime(datos_precios['FECHA'], errors='coerce')
            datos_precios = datos_precios.dropna(subset=['FECHA', 'Precio total con impuestos (€/MWh)']).sort_values(by='FECHA')
        except FileNotFoundError:
            datos_transformados["precio"] = {"error": "Archivo de datos de precios no encontrado"}
            return datos_transformados

        datos_provincia = datos_precios[datos_precios['Provincia'].str.upper() == provincia.upper()]
        if datos_provincia.empty:
            datos_transformados["precio"] = {"error": f"No se encontraron datos de precios para la provincia: {provincia}"}
            return datos_transformados

        ultimo_precio = datos_provincia['Precio total con impuestos (€/MWh)'].values[-1]
        if np.isnan(ultimo_precio):
            datos_transformados["precio"] = {"error": "El último precio contiene valores no válidos"}
            return datos_transformados

        predicciones_precio = []
        precio_actual = ultimo_precio

        for dia in rango_fechas:
            entrada_precio = np.array([[precio_actual]])
            if np.isnan(entrada_precio).any():
                entrada_precio = np.nan_to_num(entrada_precio, nan=precio_actual)
            try:
                precio_siguiente = modelo_precios.predict(entrada_precio)[0]
            except Exception as e:
                datos_transformados["precio"] = {"error": f"Error en la prediccion del dia {dia}: {str(e)}"}
                return datos_transformados
            predicciones_precio.append(precio_siguiente)
            precio_actual = precio_siguiente

        precio_medio = np.mean(predicciones_precio)
        datos_transformados["precio"] = {
            "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
            "fecha_fin": fecha_fin.strftime("%Y-%m-%d"),
            "precio_medio": precio_medio,
            "predicciones_diarias": predicciones_precio
        }
    else:
        datos_transformados["precio"] = "Modelo de precios no disponible"
        return datos_transformados

    coste_potencia = 12.91
    coste_total = 71.10
    precio_medio_kwh = datos_transformados["precio"]["precio_medio"] / 1000

    try:
        conexion = mysql.connector.connect(**db_config)
        cursor = conexion.cursor()

        id_provincia = obtener_id_dimension(cursor, "dim_provincia", "Nombre_provincia", datos.provincia, "ID_provincia", es_rango=False)
        id_vivienda = obtener_id_dimension(cursor, "dim_vivienda", "tipo_de_vivienda", datos.tipo_vivienda, "ID_vivienda", es_rango=False)
        id_potencia = obtener_id_dimension(cursor, "dim_potencia", "Potencia", datos.potencia, "ID_potencia", es_rango=True)
        id_residentes = obtener_id_dimension(cursor, "dim_residentes", "Residentes", datos.numero_residentes, "ID_residentes", es_rango=True)
        id_fecha = obtener_o_insertar_fecha(cursor)

        datos_para_base = {
            "PREDICCION_CONSUMO": datos_transformados["prediccion_consumo"],
            "PREDICCION_PRECIO": float(precio_medio_kwh),
            "COSTE_POTENCIA": coste_potencia,
            "COSTE_ESTIMADO": coste_total,
            "ID_Tiempo_Dia": id_fecha,
            "ID_vivienda": id_vivienda,
            "ID_provincia": id_provincia,
            "ID_potencia": id_potencia,
            "ID_residentes": id_residentes
        }

        cursor.execute("SELECT COALESCE(MAX(ID_predicciones), 0) FROM Datos_predicciones_SQL_ID")
        max_id = cursor.fetchone()[0]
        nuevo_id = max_id + 1

        consulta_insertar = """
        INSERT INTO Datos_predicciones_SQL_ID (
            ID_predicciones, PREDICCION_CONSUMO, PREDICCION_PRECIO, COSTE_POTENCIA, COSTE_ESTIMADO,
            ID_Tiempo_Dia, ID_vivienda, ID_provincia, ID_potencia, ID_residentes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            nuevo_id,
            datos_para_base["PREDICCION_CONSUMO"],
            datos_para_base["PREDICCION_PRECIO"],
            datos_para_base["COSTE_POTENCIA"],
            datos_para_base["COSTE_ESTIMADO"],
            datos_para_base["ID_Tiempo_Dia"],
            datos_para_base["ID_vivienda"],
            datos_para_base["ID_provincia"],
            datos_para_base["ID_potencia"],
            datos_para_base["ID_residentes"]
        )
        cursor.execute(consulta_insertar, valores)
        conexion.commit()
    except mysql.connector.Error as err:
        datos_transformados["db_error"] = f"Error al guardar en la base de datos: {str(err)}"
    except ValueError as ve:
        datos_transformados["db_error"] = str(ve)
    finally:
        try:
            while cursor.fetchone() is not None:
                pass
        except:
            pass
        cursor.close()
        conexion.close()

    return {"datos_transformados": datos_transformados}
