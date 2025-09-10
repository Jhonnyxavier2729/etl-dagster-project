from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import create_engine, exc
import os

#Asset para cargar a PostgreSQL
@asset
def postgres_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Cargar a PostgreSQL - Depende de clean_data"""
    context.log.info("🗄️ Cargando a PostgreSQL...")
    
    try:
        # ✅ Obtener URI desde variables de entorno
        postgres_uri = os.environ.get('POSTGRES_URI')
        
        if not postgres_uri:
            raise ValueError("❌ Variable de entorno POSTGRES_URI no configurada")
        
        context.log.info(f"🔗 Conectando a: {postgres_uri.split('@')[-1]}")
        
        # ✅ Usar la URI de las variables de entorno
        engine = create_engine(postgres_uri)
        
        # ✅ Cargar datos
        clean_data.to_sql('datos_limpios', engine, if_exists='replace', index=False)
        
        # ✅ Verificar que se cargaron los datos
        with engine.connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM datos_limpios").scalar()
        
        context.log.info(f"✅ Cargados {count} registros a PostgreSQL")
        return True
        
    except exc.SQLAlchemyError as e:
        context.log.error(f"❌ Error de base de datos: {e}")
        raise
    except ValueError as e:
        context.log.error(str(e))
        raise
    except Exception as e:
        context.log.error(f"❌ Error inesperado: {e}")
        raise