import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import xgboost as xgb
import os
from sklearn.preprocessing import StandardScaler
import joblib  
import matplotlib.pyplot as plt


# Carpeta base donde se encuentran los datos y donde se guardarán los modelos
base_folder = os.path.dirname(__file__)  # Obtiene la carpeta donde está el script

# Ruta relativa para los archivos de entrada (ahora apuntando a la carpeta "Datos_consumo_generados_metereologicos" en Limpieza_datos)
input_folder = os.path.join(base_folder, "..", "Limpieza_datos", "Datos_consumo_generados_meteorologicos")

# Ruta para guardar los modelos (se guardarán en una carpeta llamada "modelos_guardados" fuera de la carpeta Modelo_predicción)
output_folder = os.path.join(base_folder, "..", "modelos_guardados")

# Crear las carpetas si no existen
os.makedirs(output_folder, exist_ok=True)

# Definir una lista de provincias (esto debe coincidir con los nombres de tus archivos)
provinces = [
    "A CORUÑA", "ALAVA", "ALBACETE", "ALICANTE", "ALMERIA", "ASTURIAS", "AVILA", "BADAJOZ", "BARCELONA", "BIZKAIA",
    "BURGOS", "CACERES", "CADIZ", "CANTABRIA", "CASTELLON", "CIUDAD REAL", "CORDOBA", "CUENCA", "GIRONA", "GRANADA",
    "GUADALAJARA", "HUELVA", "HUESCA", "ILLES BALEARS", "JAEN", "LA RIOJA", "LAS PALMAS", "LEON", "LLEIDA", "LUGO",
    "MADRID", "MALAGA", "MURCIA", "NAVARRA", "OURENSE", "PALENCIA", "PONTEVEDRA", "SALAMANCA", 
    "SANTA CRUZ DE TENERIFE", "SEGOVIA", "SEVILLA", "SORIA", "TARRAGONA", "TERUEL", "TOLEDO", 
    "VALENCIA", "VALLADOLID", "ZAMORA", "ZARAGOZA"
]
# Modelos que serán considerados (filtrando aquellos que requieren normalización)
valid_models = {
    "Linear Regression": LinearRegression(),
    "Random Forest": RandomForestRegressor(),
    "Gradient Boosting": GradientBoostingRegressor(),
    "XGBoost": xgb.XGBRegressor()
}

# Iterar sobre cada provincia
for province in provinces:
    print(f"\nProcesando provincia: {province}")
    
    # Ruta del archivo de datos de la provincia
    data_file = os.path.join(input_folder, f"{province}_Consumo_Energetico_Mensual_Con_Meteorologia_Real.csv")
    
    # Verificar si el archivo existe antes de continuar
    if not os.path.exists(data_file):
        print(f"El archivo para la provincia {province} no se encuentra en la carpeta 'Datos_consumo_generados_metereologicos'. Se saltará esta provincia.")
        continue
    
    data_alava = pd.read_csv(data_file, delimiter=',')  # Cargar los datos
    
    # Preprocesar los datos
    data_alava = pd.get_dummies(data_alava, columns=["Tipo de vivienda"], drop_first=False)  # Codificar variables categóricas
    data_alava['Fecha'] = pd.to_datetime(data_alava['Fecha'], errors='coerce')  # Convertir la columna 'Fecha' a tipo datetime
    
    # Crear nuevas características a partir de la fecha
    data_alava['Mes'] = data_alava['Fecha'].dt.month
    data_alava['Year'] = data_alava['Fecha'].dt.year
    
    # Normalizar las características numéricas
    scaler = StandardScaler()
    data_normalized = scaler.fit_transform(data_alava[["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"]])
    
    # Convertir la matriz normalizada de vuelta a un DataFrame
    data_normalized_df = pd.DataFrame(data_normalized, columns=["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"])
    
    # Mantener las columnas no normalizadas, como "Mes", "Year" y "Media de residentes"
    data_final = pd.concat([data_normalized_df, data_alava[["Mes", "Media de residentes"]], data_alava.filter(like="Tipo de vivienda")], axis=1)
    # Variables independientes (X) y dependiente (y)
    X = data_final
    y = data_alava["Consumo energético (kWh/m²)"]
    print(X.columns)
    print(X.head())
    print(X.dtypes)

    # Dividir en conjuntos de entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Comparar modelos
    results = []
    best_model = None
    best_mse = float('inf')

    for model_name, model in valid_models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        results.append({"Model": model_name, "MSE": mse, "R²": r2})

        # Actualizar el mejor modelo
        if mse < best_mse:
            best_mse = mse
            best_model = model

    # Mostrar resultados de precisión de cada modelo
    results_df = pd.DataFrame(results).sort_values(by="MSE")
    print(results_df)

    # Guardar el mejor modelo para esta provincia
    model_filename = os.path.join(output_folder, f"Modelo_{province}.pkl")
    joblib.dump(best_model, model_filename)
    print(f"El mejor modelo para {province} ha sido guardado como {model_filename}")

    y_pred = best_model.predict(X_test)

    # Crear la gráfica
    plt.figure(figsize=(12, 6))
    plt.plot(range(len(y_test)), y_test, label='Consumo Real', color='blue', marker='o')
    plt.plot(range(len(y_pred)), y_pred, label='Predicción del Mejor Modelo', color='red', linestyle='--')
    plt.legend()
    plt.title(f'Predicción de Consumo Energético para {province}')
    plt.xlabel('Índice de Datos de Prueba')
    plt.ylabel('Consumo Energético (kWh/m²)')
    plt.grid()
    plt.show()
