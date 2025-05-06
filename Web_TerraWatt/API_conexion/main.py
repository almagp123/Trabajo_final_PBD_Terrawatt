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


# Ruta base del proyecto (donde está este script)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Rutas relativas
RUTA_METEOROLOGICA = os.path.join(BASE_DIR, "..", "..", "Datos_Y_Limpieza", "Datos_limpios", "Datos_limpios_metereologicos")
RUTA_MODELOS = os.path.join(BASE_DIR, "..", "..", "Modelos", "Modelos_generados", "Modelos_consumo_por_provincia")
RUTA_MODELO_PRECIOS = os.path.join(BASE_DIR, "..", "..", "Modelos", "Modelos_generados")
modelo_precios_path = os.path.join(BASE_DIR, "..", "..", "Datos_Y_Limpieza", "Datos_limpios", "Modelo_Precios_Met_Fest.csv")


# Crear la instancia de la API y configurar CORS
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Para producción: restringir orígenes
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Definir el modelo de entrada para el endpoint de transformación
class Datos(BaseModel):
    potencia: float
    numero_residentes: float
    tipo_vivienda: str
    provincia: str
    mes: int  




@app.post("/transformar")
async def transformar_datos(datos: Datos):
    # ---------------------------
    # Parte 1: Predicción de Consumo
    # ---------------------------
    # Variables dummies para el tipo de vivienda
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
        return {"error": "Tipo de vivienda no reconocido. Por favor, revisa tu entrada."}

    # Cargar y filtrar datos meteorológicos
    provincia = datos.provincia
    archivo_provincia = os.path.join(RUTA_METEOROLOGICA, f"{provincia}.csv")
    if not os.path.exists(archivo_provincia):
        return {"error": f"No se encontró el archivo de la provincia: {provincia}"}

    df_meteorologico = pd.read_csv(archivo_provincia, delimiter=";")
    df_meteorologico["MES"] = pd.to_datetime(df_meteorologico["FECHA"]).dt.month
    df_filtrado = df_meteorologico[df_meteorologico["MES"] == datos.mes]
    columnas_meteorologicas = ["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN"]
    medias_meteorologicas = df_filtrado[columnas_meteorologicas].mean()
    medias_dict = medias_meteorologicas.to_dict()

    # Construir datos transformados (para el modelo de consumo)
    datos_transformados = {
        "potencia": datos.potencia,
        "numero_residentes": datos.numero_residentes,
        "provincia": datos.provincia,
        "mes": datos.mes,
    }
    datos_transformados = {**datos_transformados, **medias_dict, **variables_vivienda}



    modelo_path = os.path.join(RUTA_MODELOS, f"Modelo_{provincia}.pkl")
    if not os.path.exists(modelo_path):
        print(f"El modelo para {provincia} no se encontró en la ruta: {modelo_path}")
        return {"error": f"No se encontró el modelo para la provincia: {provincia}"}
    modelo_consumo = joblib.load(modelo_path)

    feature_names = [
        "TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN",
        "Potencia contratada (kW)", "Mes", "Media de residentes",
        "Tipo de vivienda_Adosado", "Tipo de vivienda_Casa Unifamiliar", 
        "Tipo de vivienda_Duplex", "Tipo de vivienda_Piso"
    ]
    features = [
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
    features_df = pd.DataFrame([features], columns=feature_names)
    scaler = StandardScaler()
    features_to_normalize = ["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"]
    features_df[features_to_normalize] = scaler.fit_transform(features_df[features_to_normalize])

 

    # Realizar la predicción de consumo
    prediccion_consumo = modelo_consumo.predict(features_df)
    prediccion_consumo = max(0, prediccion_consumo[0])
    datos_transformados["prediccion_consumo"] = prediccion_consumo

    # ---------------------------
    # Parte 2: Predicción de Precio (por intervalo de fechas)
    # ---------------------------
   




#     modelo_precios_path = os.path.join(RUTA_MODELO_PRECIOS, "Modelo_precios_mlp.pkl")

#     print(f"📁 Verificando existencia del modelo de precios en: {modelo_precios_path}")
#     if os.path.exists(modelo_precios_path):
#         print("✅ Modelo de precios encontrado. Cargando...")
        
#         modelo_precios = joblib.load(modelo_precios_path)

#         # Configuración de fechas
#         año = 2025
#         fecha_inicio = datetime(año, datos.mes, 1)
#         ultimo_dia = calendar.monthrange(año, datos.mes)[1]
#         fecha_fin = datetime(año, datos.mes, ultimo_dia)

#         rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
#         n_dias = len(rango_fechas)
#         print(f"📆 Prediciendo precios para {n_dias} días: {fecha_inicio} -> {fecha_fin}")

#     # Leer archivo de datos de precios
#         file_path_precios = r"C:\Users\Alma\Desktop\Trabajo_final_PBD_Terrawatt\Datos_Y_Limpieza\Datos_limpios\Modelo_Precios_Met_Fest.csv"
#         try:
#             print(f"📄 Leyendo archivo de precios: {file_path_precios}")
#             data_precios = pd.read_csv(file_path_precios, delimiter=';')
#             data_precios['FECHA'] = pd.to_datetime(data_precios['FECHA'], errors='coerce')
#             data_precios = data_precios.dropna(subset=['FECHA', 'Precio total con impuestos (€/MWh)']).sort_values(by='FECHA')
#         except FileNotFoundError:
#             print("❌ Archivo de precios no encontrado.")
#             datos_transformados["precio"] = {"error": "Archivo de datos de precios no encontrado"}
#             return datos_transformados

#     # Filtrar datos por provincia
#         print(f"🔍 Filtrando precios para provincia: {provincia}")
#         data_provincia_precios = data_precios[data_precios['Provincia'].str.upper() == provincia.upper()]
#         if data_provincia_precios.empty:
#             print("❌ No se encontraron datos para la provincia.")
#             datos_transformados["precio"] = {"error": f"No se encontraron datos de precios para la provincia: {provincia}"}
#             return datos_transformados

#         ultimo_precio = data_provincia_precios['Precio total con impuestos (€/MWh)'].values[-1]
#         print(f"💶 Último precio disponible: {ultimo_precio}")

#         if np.isnan(ultimo_precio):
#             print("⚠️ El último precio es NaN.")
#             datos_transformados["precio"] = {"error": "El último precio contiene valores NaN"}
#             return datos_transformados

#         predicciones_precio = []
#         precio_actual = ultimo_precio

#         print("🧠 Iniciando predicciones día a día...")
#         for dia in rango_fechas:
#             entrada_precio = np.array([[precio_actual]])
#             if np.isnan(entrada_precio).any():
#                 print(f"⚠️ NaN detectado en entrada para {dia}. Se usará último precio válido.")
#                 entrada_precio = np.nan_to_num(entrada_precio, nan=precio_actual)

#             try:
#                 precio_siguiente = modelo_precios.predict(entrada_precio)[0]
#             except Exception as e:
#                 print(f"❌ Error en la predicción para {dia}: {e}")
#                 datos_transformados["precio"] = {"error": f"Error en la predicción para el día {dia}: {str(e)}"}
#                 return datos_transformados

#             predicciones_precio.append(precio_siguiente)
#             precio_actual = precio_siguiente

#             precio_medio = np.mean(predicciones_precio)
#             print(f"📊 Precio medio predicho para el mes: {precio_medio:.2f} €/MWh")

#         datos_transformados["precio"] = {
#             "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
#             "fecha_fin": fecha_fin.strftime("%Y-%m-%d"),
#             "precio_medio": precio_medio,
#             "predicciones_diarias": predicciones_precio
#         }

#     else:
#         print("❌ Modelo de precios no disponible en la ruta especificada.")
#         datos_transformados["precio"] = "Modelo de precios no disponible"


# # ------------------ Guardar en MySQL ------------------
#     # try:
#     #     precio_kwh = precio_medio / 1000
#     #     coste_potencia = datos.potencia * precio_kwh * 30
#     #     coste_energia = prediccion_consumo * precio_kwh
#     #     coste_total = coste_potencia + coste_energia

#     #     conexion = mysql.connector.connect(
#     #         host="bm2scztmsn0ysrsjzgn7-mysql.services.clever-cloud.com",
#     #         user="ucvrczudsqc894cb",
#     #         password="Dq5pAfXTFzVW7PHZi7jA",
#     #         database="bm2scztmsn0ysrsjzgn7"
#     #     )
#     #     cursor = conexion.cursor()

#     #     sql = """
#     #     INSERT INTO PREDICCIONES_CLIENTES (
#     #         POTENCIA, NRESIDENTES, TIPOVIVIENDA, PROVINCIA, MES,
#     #         PREDICCION_CONSUMO, PREDICCION_PRECIO, COSTE_POTENCIA, COSTE_ESTIMADO
#     #     ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     #     """
#     #     valores = (
#     #         datos.potencia,
#     #         datos.numero_residentes,
#     #         datos.tipo_vivienda,
#     #         datos.provincia,
#     #         datos.mes,
#     #         prediccion_consumo,
#     #         precio_kwh,
#     #         coste_potencia,
#     #         coste_total
#     #     )

#     #     cursor.execute(sql, valores)
#     #     conexion.commit()
#     #     cursor.close()
#     #     conexion.close()
#     #     print("✅ Datos guardados en MySQL correctamente.")

#     # except Exception as e:
#     #     return {"error_sql": str(e)}



#     return {"datos_transformados": datos_transformados}




    # ... (código anterior hasta la predicción de consumo)

    # ---------------------------
    # Parte 2: Predicción de Precio (por intervalo de fechas)
    # ---------------------------
    # Definir la ruta del modelo de precios al inicio
    modelo_precios_path = os.path.join(RUTA_MODELO_PRECIOS, "Modelo_precios_mlp.pkl")
    print(f"📁 Verificando existencia del modelo de precios en: {modelo_precios_path}")

    # Verificar si el modelo de precios existe
    if os.path.exists(modelo_precios_path):
        print("✅ Modelo de precios encontrado. Cargando...")
        modelo_precios = joblib.load(modelo_precios_path)

        # Configuración de fechas
        año = 2025
        fecha_inicio = datetime(año, datos.mes, 1)
        ultimo_dia = calendar.monthrange(año, datos.mes)[1]
        fecha_fin = datetime(año, datos.mes, ultimo_dia)

        rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
        n_dias = len(rango_fechas)
        print(f"📆 Prediciendo precios para {n_dias} días: {fecha_inicio} -> {fecha_fin}")

        # Leer archivo de datos de precios
        file_path_precios = r"C:\Users\Alma\Desktop\Trabajo_final_PBD_Terrawatt\Datos_Y_Limpieza\Datos_limpios\Modelo_Precios_Met_Fest.csv"
        try:
            print(f"📄 Leyendo archivo de precios: {file_path_precios}")
            data_precios = pd.read_csv(file_path_precios, delimiter=';')
            data_precios['FECHA'] = pd.to_datetime(data_precios['FECHA'], errors='coerce')
            data_precios = data_precios.dropna(subset=['FECHA', 'Precio total con impuestos (€/MWh)']).sort_values(by='FECHA')
        except FileNotFoundError:
            print("❌ Archivo de precios no encontrado.")
            datos_transformados["precio"] = {"error": "Archivo de datos de precios no encontrado"}
            return datos_transformados

        # Filtrar datos por provincia
        print(f"🔍 Filtrando precios para provincia: {provincia}")
        data_provincia_precios = data_precios[data_precios['Provincia'].str.upper() == provincia.upper()]
        if data_provincia_precios.empty:
            print("❌ No se encontraron datos para la provincia.")
            datos_transformados["precio"] = {"error": f"No se encontraron datos de precios para la provincia: {provincia}"}
            return datos_transformados

        ultimo_precio = data_provincia_precios['Precio total con impuestos (€/MWh)'].values[-1]
        print(f"💶 Último precio disponible: {ultimo_precio}")

        if np.isnan(ultimo_precio):
            print("⚠️ El último precio es NaN.")
            datos_transformados["precio"] = {"error": "El último precio contiene valores NaN"}
            return datos_transformados

        predicciones_precio = []
        precio_actual = ultimo_precio

        print("🧠 Iniciando predicciones día a día...")
        for dia in rango_fechas:
            entrada_precio = np.array([[precio_actual]])
            if np.isnan(entrada_precio).any():
                print(f"⚠️ NaN detectado en entrada para {dia}. Se usará último precio válido.")
                entrada_precio = np.nan_to_num(entrada_precio, nan=precio_actual)

            try:
                precio_siguiente = modelo_precios.predict(entrada_precio)[0]
            except Exception as e:
                print(f"❌ Error en la predicción para {dia}: {e}")
                datos_transformados["precio"] = {"error": f"Error en la predicción para el día {dia}: {str(e)}"}
                return datos_transformados

            predicciones_precio.append(precio_siguiente)
            precio_actual = precio_siguiente

        precio_medio = np.mean(predicciones_precio)
        print(f"📊 Precio medio predicho para el mes: {precio_medio:.2f} €/MWh")

        datos_transformados["precio"] = {
            "fecha_inicio": fecha_inicio.strftime("%Y-%m-%d"),
            "fecha_fin": fecha_fin.strftime("%Y-%m-%d"),
            "precio_medio": precio_medio,
            "predicciones_diarias": predicciones_precio
        }
    else:
        print("❌ Modelo de precios no disponible en la ruta especificada.")
        datos_transformados["precio"] = "Modelo de precios no disponible"

    # ... (resto del código, incluyendo la sección de MySQL comentada)

    return {"datos_transformados": datos_transformados}