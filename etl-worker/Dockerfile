FROM python:3.11-slim-bookworm

WORKDIR /app

# Instalar dependencias del sistema (MANTENER)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean


# LUEGO instalar TUS dependencias desde requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Crear directorios necesarios (MANTENER)
RUN mkdir -p /data/output /duckdb /tmp/dagster

# Health check (MANTENER)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4000/ || exit 1

CMD ["sleep", "infinity"]
