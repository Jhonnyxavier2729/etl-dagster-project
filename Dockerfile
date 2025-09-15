# Stage 1: The builder stage. This handles all heavy installations.
FROM python:3.11-slim-bookworm AS builder

# Install build-time dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /dagster_project

# Copy requirements file and install dependencies in a virtual environment
COPY requirements.txt .
RUN python -m venv /venv && \
    . /venv/bin/activate && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install "dagster==1.10.10" "dagster-webserver==1.10.10" "dagster-postgres" "dagster-docker"

# Copy the rest of the application source code
COPY . .

# Stage 2: The final, lean production image
FROM python:3.11-slim-bookworm AS final

# Install only the necessary runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /dagster_project

# Copy the virtual environment from the builder stage
COPY --from=builder /venv /venv

# Copy the application code from the builder stage
COPY --from=builder /dagster_project .

# Set environment variable PATH to use the virtual environment
ENV PATH="/venv/bin:$PATH"

# Create necessary directories and make entrypoint executable
RUN mkdir -p /tmp /duckdb 

# Expose ports 
EXPOSE 80

# Health check 
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:80/ || exit 1

CMD ["sleep", "infinity"]
