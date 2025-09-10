from dagster import asset, AssetExecutionContext
import pandas as pd
from pymongo import MongoClient
import os

#asset para cargar a MongoDB
@asset
def mongodb_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Cargar a MongoDB - Depende de clean_data"""
    context.log.info("🍃 Cargando a MongoDB...")
    
    try:
        # ✅ Obtener URI desde variables de entorno
        mongo_uri = os.environ.get('MONGO_URI')
        
        if not mongo_uri:
            raise ValueError("❌ Variable de entorno MONGO_URI no configurada")
        
        context.log.info(f"🔗 Conectando a MongoDB...")
        
        client = MongoClient(
            mongo_uri,
            authSource='admin',
            serverSelectionTimeoutMS=5000  # ✅ Timeout para conexión
        )
        
        # ✅ Verificar conexión
        client.admin.command('ping')
        context.log.info("✅ Conexión a MongoDB verificada")
        
        db = client['etl_database']
        collection = db['datos_limpios']
        
        # ✅ Contar documentos antes de borrar
        count_before = collection.count_documents({})
        if count_before > 0:
            context.log.info(f"🗑️ Eliminando {count_before} documentos existentes")
        
        collection.delete_many({})
        collection.insert_many(clean_data.to_dict('records'))
        
        # ✅ Verificar inserción
        count_after = collection.count_documents({})
        context.log.info(f"✅ Insertados {count_after} documentos en MongoDB")
        
        return True
        
    except ValueError as e:
        context.log.error(str(e))
        raise
    except Exception as e:
        context.log.error(f"❌ Error cargando a MongoDB: {e}")
        raise
    finally:
        # ✅ Cerrar conexión si existe
        if 'client' in locals():
            client.close()