FROM python:3.11-slim-bookworm

WORKDIR /opt/dagster/app

# Instalar dependencias del sistema (MANTENER)
RUN apt-get update && apt-get install -y \
    docker.io \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Instalar Dagster 1.11.9 con versiones FIJAS
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    "dagster==1.10.10" \
    "dagster-webserver==1.10.10" \
    "dagster-postgres" \
    "dagster-docker"

# Instalar dependencias de TU proyecto desde requirements.txt
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de tu proyecto
COPY .. /opt/dagster/app

# Crear directorios necesarios (MANTENER)
RUN mkdir -p /data /duckdb

# Exponer puertos (MANTENER)
EXPOSE 3000 4000

# Comando de inicio ACTUALIZADO para dagster-webserver
CMD dagster-daemon run & dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml
