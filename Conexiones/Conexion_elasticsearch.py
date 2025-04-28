import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Conexión a Elasticsearch
client = Elasticsearch(
    "http://elasticsearch-pdb1:9200",  # Cambia según tu configuración
    basic_auth=("elastic", "changeme"),
    verify_certs=False
)

# Verificar la conexión
try:
    print("Conexión a Elasticsearch exitosa:", client.info())
except Exception as e:
    print(f"Error al conectar con Elasticsearch: {e}")
    exit()

# Función para crear un índice si no existe
def create_index_if_not_exists(client, index_name):
    existing_indices = client.cat.indices(format="json")
    index_names = [index["index"] for index in existing_indices]

    if index_name not in index_names:
        client.indices.create(index=index_name)
        print(f"Índice '{index_name}' creado exitosamente.")
    else:
        print(f"El índice '{index_name}' ya existe.")

# Función para cargar datos del archivo CSV en un índice
def upload_csv_to_elasticsearch(client, file_path, index_name, id_prefix, mapping):
    # Leer el archivo CSV
    df = pd.read_csv(file_path, delimiter=';')
    print(f"Cargando {len(df)} filas desde {file_path}")

    # Crear documentos personalizados
    actions = []
    for index, row in df.iterrows():
        doc = {
            "_index": index_name,
            "_id": f"{id_prefix}_{index}",
            "_source": {field: row.get(field, None) for field in mapping}
        }
        actions.append(doc)

        # Mostrar progreso cada 500 documentos
        if index % 500 == 0:
            print(f"Preparados {index} documentos para el bulk")

    # Subir documentos a Elasticsearch usando bulk
    bulk(client, actions)
    print(f"Bulk completado. {len(actions)} documentos indexados en '{index_name}'.")

# Función para realizar una consulta de prueba
def test_index_data(client, index_name):
    print(f"\nConsultando los primeros 10 documentos del índice '{index_name}':")
    try:
        search_response = client.search(
            index=index_name,
            body={"query": {"match_all": {}}, "size": 10}
        )
        for doc in search_response["hits"]["hits"]:
            print(doc["_source"])
    except Exception as e:
        print(f"Error al consultar el índice '{index_name}': {e}")

# Crear índice para precios
create_index_if_not_exists(client, "modelo_precios_met_fest")

# Subir datos del archivo Modelo_Precios_Met_Fest.csv
precios_mapping = [
    "FECHA", "Provincia", "ALTITUD", "TMEDIA", "TMIN", "TMAX", "DIR",
    "VELMEDIA", "RACHA", "SOL", "PRESMAX", "PRESMIN",
    "Precio total con impuestos (€/MWh)", "Festivo", "Entre semana"
]
upload_csv_to_elasticsearch(
    client,
    "./Modelo_Precios_Met_Fest.csv",
    "modelo_precios_met_fest",
    "precios",
    precios_mapping
)

# Comprobar los datos cargados en el índice de precios
test_index_data(client, "modelo_precios_met_fest")
