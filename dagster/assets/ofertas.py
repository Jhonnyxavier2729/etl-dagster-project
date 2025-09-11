from dagster import asset, AssetExecutionContext
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, exc
import duckdb
import os

@asset(group_name="ofertas")
def raw_data_ofertas(context: AssetExecutionContext):
    """Extraer datos del CSV con detección automática inteligente"""
    context.log.info("📥 Extrayendo datos del CSV ofertas...")
    
    try:
        file_path = '/data/input/ofertas.csv'
        
        # ✅ MÉTODO INTELIGENTE - pandas detecta automáticamente
        df = pd.read_csv(file_path, sep=None, engine='python')
        
        context.log.info("✅ CSV leído con detección automática de delimitador")
        context.log.info(f"📊 Datos extraídos: {len(df)} registros, {len(df.columns)} columnas")
        context.log.info(f"📝 Columnas: {list(df.columns)}")
        
        # Información sobre tipos de datos
        context.log.info("🔍 Tipos de datos iniciales:")
        for col, dtype in df.dtypes.items():
            context.log.info(f"   - {col}: {dtype}")
        
        return df
        
    except Exception as e:
        context.log.error(f"❌ Error en extracción: {str(e)}")
        raise


#asset para transformar y limpiar datos
@asset(group_name="ofertas")
def clean_data_ofertas(context: AssetExecutionContext, raw_data_ofertas: pd.DataFrame):
    """Transformar y limpiar datos - Depende de raw_data"""
    context.log.info("🔄 Iniciando transformación de datos...")
    
    try:
        # Hacer copia del DataFrame original
        df_clean = raw_data_ofertas.copy()
        
        context.log.info(f"📥 Datos recibidos: {df_clean.shape[0]} filas x {df_clean.shape[1]} columnas")
        
        # ❌ 1. Convertir todas las columnas a string
        context.log.info("🔄 Convirtiendo todas las columnas a string...")
        df_clean = df_clean.astype(str)
        context.log.info(f"Tipos de columnas: {df_clean.dtypes.to_dict()}")


        # 2. ✅ Ahora reemplazar TODOS los valores "vacíos"
        context.log.info("🔄 Reemplazando valores vacíos por 'NA'...")
        df_clean = df_clean.replace({
            'nan': 'NA',      # NaN convertidos a string
            'None': 'NA',     # None convertidos a string  
            '': 'NA',         # Strings vacíos
            ' ': 'NA',        # Espacios en blanco
            '  ': 'NA',       # Múltiples espacios
            '@': 'NA',         # Arrobas sueltas
            '0000000': 'NA'   # Ceros sueltos

        })
        
        
        # Log de resultados
        context.log.info("✅ Transformación completada:")
        context.log.info(f"   - Registros finales: {len(df_clean)}")
        context.log.info(f"   - Columnas: {len(df_clean.columns)}")
        context.log.info(f"   - Tipos de datos: Todos convertidos a string")
        
        # Muestra de verificación
        context.log.info("👀 Vista previa de datos limpios:")
        for col in df_clean.columns[:3]:  # Primeras 3 columnas
            unique_count = df_clean[col].nunique()
            context.log.info(f"   - {col}: {unique_count} valores únicos")
        
        return df_clean
        
    except Exception as e:
        context.log.error(f"❌ Error crítico en transformación: {str(e)}")
        context.log.error(f"📊 Estado del DataFrame durante el error: {df_clean.shape if 'df_clean' in locals() else 'No definido'}")
        raise



#Asset para guardar el CSV limpio
@asset(group_name="ofertas")
def clean_csv_data_ofertas(context: AssetExecutionContext, clean_data_ofertas: pd.DataFrame):
    """Guardar CSV limpio - Depende de clean_data"""
    context.log.info("💾 Guardando CSV limpio...")
    try:
        output_path = '/data/output/ofertas_limpio.csv'
        clean_data_ofertas.to_csv(output_path, index=False, encoding='utf-8')
        context.log.info(f"✅ CSV guardado en: {output_path}")
        return output_path
    except Exception as e:
        context.log.error(f"❌ Error guardando CSV: {e}")
        raise


#asset para cargar a MongoDB
@asset(group_name="ofertas")
def mongodb_data_ofertas(context: AssetExecutionContext, clean_data_ofertas: pd.DataFrame):
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
        collection = db['ofertas_limpios']
        
        # ✅ Contar documentos antes de borrar
        count_before = collection.count_documents({})
        if count_before > 0:
            context.log.info(f"🗑️ Eliminando {count_before} documentos existentes")
        
        collection.delete_many({})
        collection.insert_many(clean_data_ofertas.to_dict('records'))
        
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

    
#Asset para cargar a PostgreSQL
@asset(group_name="ofertas")
def postgres_data_ofertas(context: AssetExecutionContext, clean_data_ofertas: pd.DataFrame):
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
        clean_data_ofertas.to_sql('ofertas_limpios', engine, if_exists='replace', index=False)

        # ✅ Verificar que se cargaron los datos
        with engine.connect() as conn:
            count = conn.execute("SELECT COUNT(*) FROM ofertas_limpios").scalar()
        
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


#asset para cargar a duckdb
@asset(group_name="ofertas")
def duckdb_data_ofertas(context: AssetExecutionContext, clean_data_ofertas: pd.DataFrame):
    """Cargar a DuckDB - Depende de clean_data"""
    context.log.info("🦆 Cargando a DuckDB...")
    
    try:
        # Volver a la conexión local (no por red)
        con = duckdb.connect('/duckdb/data.db')  # ← ARCHIVO LOCAL
        context.log.info("✅ Conectado a DuckDB local")

        con.register('temp_clean_data', clean_data_ofertas)
        con.execute("CREATE OR REPLACE TABLE ofertas_limpios AS SELECT * FROM temp_clean_data")

        count = con.execute("SELECT COUNT(*) FROM ofertas_limpios").fetchone()[0]
        context.log.info(f"📊 {count} registros insertados")
        
        con.close()
        return True
        
    except Exception as e:
        context.log.error(f"❌ Error cargando a DuckDB: {e}")
        raise