import pandas as pd
from sqlalchemy import create_engine
import os

user = "ucvrczudsqc894cb"
password = "Dq5pAfXTFzVW7PHZi7jA"
host = "bm2scztmsn0ysrsjzgn7-mysql.services.clever-cloud.com"
port = "3306"
database = "bm2scztmsn0ysrsjzgn7"

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


csv_path = "../TerraWatt/Terrawatt/Limpieza_datos/Datos_precios_SQL_ID.csv"


print(f"📥 Cargando archivo: {csv_path}")
df = pd.read_csv(csv_path, sep=';')

df.columns = df.columns.str.strip().str.replace(" ", "_")

df.to_sql("Datos_precios", con=engine, if_exists='append', index=False)
print("✅ Archivo cargado correctamente en la tabla 'Datos_precios'.")
