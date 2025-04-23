import pandas as pd
import numpy as np
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import pickle
import os 
# 1. Cargar el dataset
file_path = "../Limpieza_datos/Modelo_Precios_Met_Fest.csv"
data = pd.read_csv(file_path, delimiter=';')

# 2. Preprocesamiento del dataset
data['FECHA'] = pd.to_datetime(data['FECHA'])
data = data.sort_values(by='FECHA')

# Usaremos la columna "Precio total con impuestos (€/MWh)" como serie temporal
serie_temporal = data.set_index('FECHA')['Precio total con impuestos (€/MWh)'].dropna()

# 3. Construir el dataset para predicción
# Usaremos la técnica de ventanas de tamaño 1:
#   X[i] = precio en t, y[i] = precio en t+1

prices = serie_temporal.values
X = prices[:-1].reshape(-1, 1)  # características: precio actual

y = prices[1:]                  # etiquetas: precio siguiente

# Dividir en conjunto de entrenamiento y prueba (80% - 20%)
train_size = int(len(X) * 0.8)
X_train, X_test = X[:train_size], X[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# 4. Definir y entrenar el modelo usando MLPRegressor de scikit-learn
model = MLPRegressor(hidden_layer_sizes=(100, 50), 
                     activation='relu', 
                     solver='adam', 
                     max_iter=500, 
                     random_state=42)

model.fit(X_train, y_train)

# 5. Realizar predicciones
predictions = model.predict(X_test)

# Evaluar el modelo
rmse = np.sqrt(mean_squared_error(y_test, predictions))
r2 = r2_score(y_test, predictions)
print(f"RMSE: {rmse:.2f}")
print(f"R²: {r2:.2f}")

# 6. Visualización de los resultados
plt.figure(figsize=(10, 6))
plt.plot(serie_temporal.index, serie_temporal.values, label='Datos Reales')
# Ajustamos el índice para las predicciones. Notar que hay una diferencia de 1 punto.
plt.plot(serie_temporal.index[train_size+1:], predictions, label='Predicción MLP', color='red')
plt.legend()
plt.title('Predicción de Precios con MLPRegressor')
plt.xlabel('Fecha')
plt.ylabel('Precio (€/MWh)')
plt.show()



# Ruta del archivo .pkl
modelo_pkl_path = "../TerraWatt/Terrawatt/modelos_guardados/AModelo_precios_mlp.pkl"

# Crear la carpeta si no existe
directorio = os.path.dirname(modelo_pkl_path)
if not os.path.exists(directorio):
    os.makedirs(directorio)
    print(f"Carpeta creada: {directorio}")

# Guardar el modelo en formato .pkl
with open(modelo_pkl_path, "wb") as f:
    pickle.dump(model, f)

print("Modelo guardado en formato .pkl exitosamente.")
