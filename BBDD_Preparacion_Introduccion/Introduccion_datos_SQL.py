import pandas as pd
from sqlalchemy import create_engine
import os

# user="pdb3_22144755"
# password="alma#Grupo3"
# host="195.235.211.197"
# port="3306"
# database="pdb3_grupo3"

# engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


# csv_path = "../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_precios_SQL_ID.csv"


# print(f"📥 Cargando archivo: {csv_path}")
# df = pd.read_csv(csv_path, sep=';')

# df.columns = df.columns.str.strip().str.replace(" ", "_")

# df.to_sql("Datos_precios", con=engine, if_exists='append', index=False)
# print("Archivo cargado correctamente en la tabla 'Datos_precios'.")


# import pandas as pd
# from sqlalchemy import create_engine
# import pymysql

# # Conexión
# user = "pdb3_22144755"
# password = "alma#Grupo3"
# host = "195.235.211.197"
# port = 3306
# database = "pdb3_grupo3"

# engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
# conn = engine.raw_connection()
# cursor = conn.cursor()

# # Leer CSV
# df = pd.read_csv("../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_precios_SQL_ID.csv", sep=';')
# df.columns = df.columns.str.strip().str.replace(" ", "_")

# # SQL compacto con ON DUPLICATE KEY UPDATE
# cols = ",".join(df.columns)
# updates = ",".join([f"{col}=VALUES({col})" for col in df.columns if col != "ID"])
# placeholders = ",".join(["%s"] * len(df.columns))
# sql = f"INSERT INTO Datos_precios ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"

# cursor.executemany(sql, df.to_records(index=False).tolist())
# conn.commit()
# cursor.close()
# conn.close()

# print("✅ Datos insertados/actualizados correctamente.")

# ------------------------------
# import pandas as pd
# from sqlalchemy import create_engine

# user="pdb3_22144755"
# password="alma#Grupo3"
# host="195.235.211.197"
# port="3306"
# database="pdb3_grupo3"

# engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# csv_path = "../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_consumo_SQL_ID.csv"

# print(f"📥 Cargando archivo: {csv_path}")
# df = pd.read_csv(csv_path, sep=';', encoding='utf-8')

# df.columns = df.columns.str.strip().str.replace(" ", "_")

# df.to_sql("Datos_consumo", con=engine, if_exists='append', index=False)
# print("✅ Archivo cargado correctamente en la tabla 'Datos_consumo'.")



# # ------------------------------
# import pandas as pd
# from sqlalchemy import create_engine

# # Conexión

# # Lista de provincias
# provincias = [
#     'A CORUÑA', 'ALAVA', 'ALBACETE', 'ALICANTE', 'ALMERIA', 'ASTURIAS', 'AVILA', 'BADAJOZ', 'BARCELONA',
#     'BIZKAIA', 'BURGOS', 'CACERES', 'CADIZ', 'CANTABRIA', 'CASTELLON', 'CIUDAD REAL', 'CORDOBA', 'CUENCA',
#     'GIRONA', 'GRANADA', 'GUADALAJARA', 'HUELVA', 'HUESCA', 'ILLES BALEARS', 'JAEN', 'LA RIOJA',
#     'LAS PALMAS', 'LEON', 'LLEIDA', 'LUGO', 'MADRID', 'MALAGA', 'MURCIA', 'NAVARRA', 'OURENSE',
#     'PALENCIA', 'PONTEVEDRA', 'SALAMANCA', 'SANTA CRUZ DE TENERIFE', 'SEGOVIA', 'SEVILLA', 'SORIA',
#     'TARRAGONA', 'TERUEL', 'TOLEDO', 'VALENCIA', 'VALLADOLID', 'ZAMORA', 'ZARAGOZA'
# ]

# # Crear DataFrame
# df_prov = pd.DataFrame(provincias, columns=["nombre_provincia"])
# df_prov.index += 1
# df_prov.reset_index(inplace=True)
# df_prov.rename(columns={"index": "id"}, inplace=True)

# # Subir la tabla a MySQL (esto crea y sube)
# df_prov.to_sql("dim_provincia", con=engine, if_exists="append", index=False)
# print("✅ Tabla 'dim_provincia' creada correctamente en MySQL.")


# # ------------------------------
# # ------------------------------
# # 📦 Cargar predicciones generadas en MySQL
# # ------------------------------

# import pandas as pd
# from sqlalchemy import create_engine




# # Crear conexión con SQLAlchemy
# engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# # Ruta al CSV de predicciones generadas
# csv_path = "../TerraWatt/Terrawatt/Limpieza_datos/BBDD_SQL/Datos_sinteticos_prediccion.csv"

# print(f"📥 Cargando archivo: {csv_path}")
# df = pd.read_csv(csv_path, sep=';', encoding='utf-8')

# # Normalizar nombres de columnas
# df.columns = df.columns.str.strip().str.replace(" ", "_")

# # Cargar a MySQL en tabla "PREDICCIONES_CLIENTES"
# df.to_sql("PREDICCIONES_CLIENTES", con=engine, if_exists='append', index=False)
# print("✅ Archivo cargado correctamente en la tabla 'PREDICCIONES_CLIENTES'.")

