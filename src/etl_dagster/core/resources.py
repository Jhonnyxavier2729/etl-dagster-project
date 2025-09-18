import os
from dagster import resource
from dagster_duckdb import DuckDBResource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pymongo import MongoClient
from pymongo.database import Database

@resource
def postgres_resource() -> Engine:
    """SQLAlchemy Engine for ETL Postgres (reads real env var)."""
    postgres_uri = os.getenv("POSTGRES_URI")
    if not postgres_uri:
        raise ValueError("POSTGRES_URI environment variable is not set")
    try:
        engine = create_engine(postgres_uri)
    except Exception as e:
        raise ValueError(f"Invalid POSTGRES_URI='{postgres_uri}': {e}") from e
    return engine

@resource
def mongodb_resource() -> Database:
    """PyMongo Database instance from MONGO_URI"""
    mongo_uri = os.getenv("MONGO_URI", "")
    print(f"MONGO_URI: {mongo_uri}")
    if not mongo_uri:
        raise ValueError("MONGO_URI environment variable is not set")
    client = MongoClient(mongo_uri)
    # get db name from env or URI path
    db_name = os.getenv("MONGO_DB") or MongoClient(mongo_uri).get_default_database().name if '/' in mongo_uri and mongo_uri.rstrip('/').split('/')[-1] else None
    if not db_name:
        # try parsing path
        from urllib.parse import urlparse
        parsed = urlparse(mongo_uri)
        db_name = parsed.path.lstrip("/")
    if not db_name:
        raise ValueError("Database name not found in MONGO_URI and MONGO_DB not set")
    return client[db_name]

@resource
def duckdb_resource() -> DuckDBResource:
    """Dagster DuckDBResource configured from DUCKDB_PATH env var."""
    duckdb_path = os.getenv("DUCKDB_PATH", "/dagster_project/data/duckdb/etl_database.db")
    return DuckDBResource(database=duckdb_path)


# Instantiate all resources for use in definitions
all_resources = {
    "postgres_resource": postgres_resource,
    "mongodb_resource": mongodb_resource,
    "duckdb_resource": duckdb_resource,
}
