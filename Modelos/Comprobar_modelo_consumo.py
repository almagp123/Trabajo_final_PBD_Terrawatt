import joblib
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Cargar el modelo
modelo_path = '../TerraWatt/Terrawatt/modelos_guardados/Modelo_ALAVA.pkl'  # Cambia esta ruta a la correcta
modelo = joblib.load(modelo_path)


datos_entrada = {
    "TMEDIA": 5.31125,
    "TMIN": 1.3264919354838711,
    "TMAX": 9.296043906810036,
    "VELMEDIA": 2.4746397849462367,
    "SOL": 3.3099999999999996,
    "PRESMAX": 965.4137096774193,
    "PRESMIN": 960.1672580645161,
    "Potencia contratada (kW)": 2,  
    "Mes": 1,  
    "Media de residentes": 4,  
    "Tipo de vivienda_Adosado": False,
    "Tipo de vivienda_Casa Unifamiliar": False,
    "Tipo de vivienda_Duplex": False,
    "Tipo de vivienda_Piso": True  
}

feature_names = [
    "TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN",
    "Potencia contratada (kW)", "Mes", "Media de residentes",
    "Tipo de vivienda_Adosado", "Tipo de vivienda_Casa Unifamiliar", 
    "Tipo de vivienda_Duplex", "Tipo de vivienda_Piso"
]

features = [datos_entrada[key] for key in feature_names]
features_df = pd.DataFrame([features], columns=feature_names)

scaler = StandardScaler()

features_to_normalize = ["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)", "Mes", "Media de residentes"]

features_df[features_to_normalize] = scaler.fit_transform(features_df[features_to_normalize])

prediccion = modelo.predict(features_df)

prediccion = max(0, prediccion[0])

print(f"La predicción de consumo energético es: {prediccion} kWh")
