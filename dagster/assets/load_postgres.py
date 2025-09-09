from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import create_engine
import os

@asset
def postgres_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Cargar a PostgreSQL - Depende de clean_data"""
    context.log.info("🗄️ Cargando a PostgreSQL...")
    try:
        os.makedirs('/data/backup/postgres', exist_ok=True)
        
        engine = create_engine('postgresql://etl_user:etl_password_2024@postgres-db:5432/etl_database')
        clean_data.to_sql('datos_limpios', engine, if_exists='replace', index=False)
        
        return True
        
    except Exception as e:
        context.log.error(f"❌ Error cargando a PostgreSQL: {e}")
        raise