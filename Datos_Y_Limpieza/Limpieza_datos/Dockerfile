FROM python:3.9.20-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir requests pandas unidecode elasticsearch urllib3


CMD ["sh", "-c", "\
                    python Extraccion_precios_energia.py && \
                    python Datos_limpieza_meteorologicos.py && \
                    python Generacion_csv_modelos.py && \
                    python Conexion_elasticsearch.py &&\
                    tail -f /dev/null"]