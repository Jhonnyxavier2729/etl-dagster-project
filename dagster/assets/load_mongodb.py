from dagster import asset, AssetExecutionContext
import pandas as pd
from pymongo import MongoClient
import os

@asset
def mongodb_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Cargar a MongoDB - Depende de clean_data"""
    context.log.info("🍃 Cargando a MongoDB...")
    try:
        os.makedirs('/data/backup/mongodb', exist_ok=True)
        
        client = MongoClient(
            'mongodb://admin:mongo_password_2024@mongodb:27017/',
            authSource='admin'
        )
        db = client['etl_database']
        collection = db['datos_limpios']
        collection.delete_many({})
        collection.insert_many(clean_data.to_dict('records'))
        
        return True
        
    except Exception as e:
        context.log.error(f"❌ Error cargando a MongoDB: {e}")
        raise