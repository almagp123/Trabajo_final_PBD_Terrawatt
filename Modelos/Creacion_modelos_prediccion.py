import pandas as pd
import numpy as np
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import xgboost as xgb
import os
from sklearn.preprocessing import StandardScaler
import joblib
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from sqlalchemy import create_engine, text

# === CREDENCIALES DE MySQL ===
user = "uhmzmxoizkatmdsu"
password = "hcG4aHLWkwV4KrjM9re"
host = "hv-par8-022.clvrcld.net"
port = "10532"
database = "brqtr1tzuvatzxwisgpf"

# === CREAR CONEXIÓN A MySQL ===
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# === DEFINIR TABLAS EN MySQL ===
# Tabla para precios
table_definition_precios = """
CREATE TABLE Error_modelo_precios (
    ID_modelo INT PRIMARY KEY AUTO_INCREMENT,
    Fecha_generacion DATETIME,
    RMSE FLOAT,
    R2 FLOAT,
    Accuracy FLOAT,
    MAE FLOAT
)
"""

# Tabla para consumo
table_definition_consumo = """
CREATE TABLE Error_modelo_consumo (
    ID_modelo INT PRIMARY KEY AUTO_INCREMENT,
    Fecha_generacion DATETIME,
    RMSE FLOAT,
    R2 FLOAT,
    MAE FLOAT,
    Comunidad VARCHAR(50)
)
"""

print("Configurando tablas en MySQL...")
with engine.connect() as connection:
    # Configurar tabla para precios
    result_precios = connection.execute(text("SHOW TABLES LIKE 'Error_modelo_precios'"))
    if not result_precios.fetchone():
        connection.execute(text(table_definition_precios))
        print("✅ Tabla 'Error_modelo_precios' creada en MySQL.")
    else:
        print("✅ Tabla 'Error_modelo_precios' ya existe.")

    # Configurar tabla para consumo
    result_consumo = connection.execute(text("SHOW TABLES LIKE 'Error_modelo_consumo'"))
    if not result_consumo.fetchone():
        connection.execute(text(table_definition_consumo))
        print("✅ Tabla 'Error_modelo_consumo' creada en MySQL.")
    else:
        print("✅ Tabla 'Error_modelo_consumo' ya existe.")

# === DEFINIR RUTAS BASE ===
base_folder = os.path.dirname(__file__)  # Obtiene la carpeta donde está el script

# Rutas para los datos
price_file_path = os.path.join(base_folder, "..", "Datos_Y_Limpieza", "Datos_limpios", "Modelo_Precios_Met_Fest.csv")
input_folder_consumo = os.path.join(base_folder, "..", "Datos_Y_Limpieza", "Datos_limpios", "Datos_consumo_generados_meteorologicos")
print(f"Directorio de entrada para consumo configurado como: {input_folder_consumo}")

# Ruta para guardar los modelos
output_folder = os.path.join(base_folder, "..",  "Modelos", "Modelos_generados", "Modelos_consumo_por_provincia")
os.makedirs(output_folder, exist_ok=True)

# === FUNCIÓN PARA ENTRENAR EL MODELO DE PRECIOS ===
def train_price_model():
    print("\n=== Entrenando Modelo de Precios ===")
    
    # Cargar datos
    data = pd.read_csv(price_file_path, delimiter=';')

    # Preprocesamiento
    data['FECHA'] = pd.to_datetime(data['FECHA'])
    data = data.sort_values(by='FECHA')
    data = data[['FECHA', 'Precio total con impuestos (€/MWh)']].dropna()

    # Usaremos la columna "Precio total con impuestos (€/MWh)" como serie temporal
    serie_temporal = data.set_index('FECHA')['Precio total con impuestos (€/MWh)']

    # Construir el dataset para predicción
    prices = serie_temporal.values
    X = prices[:-1].reshape(-1, 1)
    y = prices[1:]

    # Dividir en entrenamiento y prueba (90% entrenamiento, 10% prueba)
    train_size = int(len(X) * 0.9)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    # Definir el modelo
    model = MLPRegressor(
        hidden_layer_sizes=(5,), 
        activation='relu',
        solver='adam',
        max_iter=100, 
        alpha=1.0,  
        early_stopping=True,
        validation_fraction=0.1,
        random_state=42
    )

    # Entrenar el modelo
    print("🔍 Entrenando modelo de precios...")
    model.fit(X_train, y_train)

    # Realizar predicciones
    predictions = model.predict(X_test)

    # Calcular métricas
    print("Métricas del modelo de precios:")
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    r2 = r2_score(y_test, predictions)
    tolerance = 0.05  
    correct_predictions = np.abs(y_test - predictions) <= tolerance * np.abs(y_test)
    accuracy = (np.sum(correct_predictions) / len(y_test)) * 100
    mae = mean_absolute_error(y_test, predictions)

    print(f"RMSE: {rmse:.2f} €/MWh")
    print(f"R²: {r2:.2f}")
    print(f"Accuracy (dentro del 5%): {accuracy:.2f}%")
    print(f"MAE: {mae:.2f} €/MWh")

    # Guardar métricas en MySQL
    print("Guardando métricas de precios en MySQL...")
    model_metrics = pd.DataFrame([{
        'Fecha_generacion': datetime.now(),
        'RMSE': rmse,
        'R2': r2,
        'Accuracy': accuracy,
        'MAE': mae
    }])
    model_metrics.to_sql('Error_modelo_precios', con=engine, if_exists='append', index=False)
    print(f"Métricas guardadas en 'Error_modelo_precios' con {len(model_metrics)} registro.")

    # Guardar el modelo en formato .pkl
    print("Guardando modelo de precios en formato .pkl...")
    modelo_pkl_path = os.path.join(output_folder, "Modelo_precios_mlp.pkl")
    with open(modelo_pkl_path, "wb") as f:
        pickle.dump(model, f)
    print("✅ Modelo de precios guardado en formato .pkl exitosamente.")

    # Visualización (opcional, descomentar para usar)
    """
    plt.figure(figsize=(10, 6))
    plt.plot(serie_temporal.index[train_size+1:], serie_temporal.values[train_size+1:], label='Datos Reales')
    plt.plot(serie_temporal.index[train_size+1:], predictions, label='Predicción MLP', color='red')
    plt.legend()
    plt.title('Predicción de Precios con MLPRegressor')
    plt.xlabel('Fecha')
    plt.ylabel('Precio (€/MWh)')
    plt.show()
    """

# === FUNCIÓN PARA ENTRENAR EL MODELO DE CONSUMO ===
def train_consumption_model():
    print("\n=== Entrenando Modelo de Consumo ===")

    # Definir una lista de provincias
    provinces = [
        "A CORUÑA", "ALAVA", "ALBACETE", "ALICANTE", "ALMERIA", "ASTURIAS", "AVILA", "BADAJOZ", "BARCELONA", "BIZKAIA",
        "BURGOS", "CACERES", "CADIZ", "CANTABRIA", "CASTELLON", "CIUDAD REAL", "CORDOBA", "CUENCA", "GIRONA", "GRANADA",
        "GUADALAJARA", "HUELVA", "HUESCA", "ILLES BALEARS", "JAEN", "LA RIOJA", "LAS PALMAS", "LEON", "LLEIDA", "LUGO",
        "MADRID", "MALAGA", "MURCIA", "NAVARRA", "OURENSE", "PALENCIA", "PONTEVEDRA", "SALAMANCA", 
        "SANTA CRUZ DE TENERIFE", "SEGOVIA", "SEVILLA", "SORIA", "TARRAGONA", "TERUEL", "TOLEDO", 
        "VALENCIA", "VALLADOLID", "ZAMORA", "ZARAGOZA"
    ]

    # Modelos que serán considerados
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
        data_file = os.path.join(input_folder_consumo, f"{province}_Consumo_Energetico_Mensual_Con_Meteorologia_Real.csv")
        
        # Verificar si el archivo existe antes de continuar
        if not os.path.exists(data_file):
            print(f"El archivo para la provincia {province} no se encuentra en {input_folder_consumo}. Verificando contenido del directorio...")
            if os.path.exists(input_folder_consumo):
                print(f"Directorio existe. Archivos encontrados: {os.listdir(input_folder_consumo)}")
            else:
                print(f"Directorio {input_folder_consumo} no existe.")
            continue
        
        data = pd.read_csv(data_file, delimiter=',')  # Cargar los datos
        
        # Preprocesar los datos
        data = pd.get_dummies(data, columns=["Tipo de vivienda"], drop_first=False)  # Codificar variables categóricas
        data['Fecha'] = pd.to_datetime(data['Fecha'], errors='coerce')  # Convertir la columna 'Fecha' a tipo datetime
        
        # Crear nuevas características a partir de la fecha
        data['Mes'] = data['Fecha'].dt.month
        data['Year'] = data['Fecha'].dt.year
        
        # Normalizar las características numéricas
        scaler = StandardScaler()
        data_normalized = scaler.fit_transform(data[["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"]])
        
        # Convertir la matriz normalizada de vuelta a un DataFrame
        data_normalized_df = pd.DataFrame(data_normalized, columns=["TMEDIA", "TMIN", "TMAX", "VELMEDIA", "SOL", "PRESMAX", "PRESMIN", "Potencia contratada (kW)"])
        
        # Mantener las columnas no normalizadas
        data_final = pd.concat([data_normalized_df, data[['Mes', 'Media de residentes']], data.filter(like="Tipo de vivienda")], axis=1)
        
        # Variables independientes (X) y dependiente (y)
        X = data_final
        y = data["Consumo energético (kWh/m²)"]
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

        # Calcular métricas para el mejor modelo
        y_pred_best = best_model.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred_best))
        r2 = r2_score(y_test, y_pred_best)
        mae = mean_absolute_error(y_test, y_pred_best)

        print(f"Métricas del mejor modelo para {province}:")
        print(f"RMSE: {rmse:.2f} kWh/m²")
        print(f"R²: {r2:.2f}")
        print(f"MAE: {mae:.2f} kWh/m²")

        # Guardar métricas en MySQL para el mejor modelo
        print("Guardando métricas de consumo en MySQL...")
        model_metrics = pd.DataFrame([{
            'Fecha_generacion': datetime.now(),
            'RMSE': rmse,
            'R2': r2,
            'MAE': mae,
            'Comunidad': province
        }])
        model_metrics.to_sql('Error_modelo_consumo', con=engine, if_exists='append', index=False)
        print(f"Métricas guardadas en 'Error_modelo_consumo' para {province}.")

        # Guardar el mejor modelo para esta provincia
        model_filename = os.path.join(output_folder, f"Modelo_{province}.pkl")
        joblib.dump(best_model, model_filename)
        print(f"El mejor modelo para {province} ha sido guardado como {model_filename}")

        # Visualización (opcional, descomentar para usar)
        """
        plt.figure(figsize=(12, 6))
        plt.plot(range(len(y_test)), y_test, label='Consumo Real', color='blue', marker='o')
        plt.plot(range(len(y_pred_best)), y_pred_best, label='Predicción del Mejor Modelo', color='red', linestyle='--')
        plt.legend()
        plt.title(f'Predicción de Consumo Energético para {province}')
        plt.xlabel('Índice de Datos de Prueba')
        plt.ylabel('Consumo Energético (kWh/m²)')
        plt.grid()
        plt.show()
        """

# === EJECUTAR AMBOS MODELOS ===
if __name__ == "__main__":
    # Entrenar modelo de precios
    train_price_model()

    # Entrenar modelo de consumo
    train_consumption_model()

    print("\n✅ Ejecución completada. Ambos modelos han sido entrenados y las métricas guardadas en MySQL.")