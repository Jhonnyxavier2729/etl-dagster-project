from dagster import asset, AssetExecutionContext
import pandas as pd
from sqlalchemy import exc, text


@asset(group_name="centro")
def raw_data_centro(context: AssetExecutionContext):
    """Extraer datos del CSV con detección automática inteligente"""
    context.log.info("📥 Extrayendo datos del CSV centro...")

    try:
        file_path = "/dagster_project/data/input/centro.csv"

        # ✅ MÉTODO INTELIGENTE - pandas detecta automáticamente
        df = pd.read_csv(file_path, sep=None, engine="python")

        context.log.info("✅ CSV leído con detección automática de delimitador")
        context.log.info(
            f"📊 Datos extraídos: {len(df)} registros, {len(df.columns)} columnas"
        )
        context.log.info(f"📝 Columnas: {list(df.columns)}")

        # Información sobre tipos de datos
        context.log.info("🔍 Tipos de datos iniciales:")
        for col, dtype in df.dtypes.items():
            context.log.info(f"   - {col}: {dtype}")

        return df

    except Exception as e:
        context.log.error(f"❌ Error en extracción: {str(e)}")
        raise


# asset para transformar y limpiar datos
@asset(group_name="centro")
def clean_data_centro(context: AssetExecutionContext, raw_data_centro: pd.DataFrame):
    """Transformar y limpiar datos - Depende de raw_data"""
    context.log.info("🔄 Iniciando transformación de datos...")

    try:
        # Hacer copia del DataFrame original
        df_clean = raw_data_centro.copy()

        context.log.info(
            f"📥 Datos recibidos: {df_clean.shape[0]} filas x {df_clean.shape[1]} columnas"
        )

        # ❌ 1. Convertir todas las columnas a string
        context.log.info("🔄 Convirtiendo todas las columnas a string...")
        df_clean = df_clean.astype(str)
        context.log.info(f"Tipos de columnas: {df_clean.dtypes.to_dict()}")

        # 2. ✅ Ahora reemplazar TODOS los valores "vacíos"
        context.log.info("🔄 Reemplazando valores vacíos por 'NA'...")
        df_clean = df_clean.replace(
            {
                "nan": "NA",  # NaN convertidos a string
                "None": "NA",  # None convertidos a string
                "": "NA",  # Strings vacíos
                " ": "NA",  # Espacios en blanco
                "  ": "NA",  # Múltiples espacios
            }
        )

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
        context.log.error(
            f"📊 Estado del DataFrame durante el error: {df_clean.shape if 'df_clean' in locals() else 'No definido'}"
        )
        raise


# Asset para guardar el CSV limpio
@asset(group_name="centro")
def clean_csv_data_centro(
    context: AssetExecutionContext, clean_data_centro: pd.DataFrame
):
    """Guardar CSV limpio - Depende de clean_data"""
    context.log.info("💾 Guardando CSV limpio...")
    try:
        output_path = "/dagster_project/data/output/centro_limpio.csv"
        clean_data_centro.to_csv(output_path, index=False, encoding="utf-8")
        context.log.info(f"✅ CSV guardado en: {output_path}")
        return output_path
    except Exception as e:
        context.log.error(f"❌ Error guardando CSV: {e}")
        raise


# asset para cargar a MongoDB
@asset(group_name="centro", required_resource_keys={"mongodb_resource"})
def mongodb_data_centro(
    context: AssetExecutionContext,
    clean_data_centro: pd.DataFrame,
):
    """Cargar a MongoDB - Depende de clean_data"""
    context.log.info("🍃 Cargando a MongoDB...")

    try:
        # ✅ Usar el recurso de MongoDB
        db = context.resources.mongodb_resource
        collection = db["centro_limpios"]

        # ✅ Contar documentos antes de borrar
        count_before = collection.count_documents({})
        if count_before > 0:
            context.log.info(f"🗑️ Eliminando {count_before} documentos existentes")

        collection.delete_many({})
        collection.insert_many(clean_data_centro.to_dict("records"))

        # ✅ Verificar inserción
        count_after = collection.count_documents({})
        context.log.info(f"✅ Insertados {count_after} documentos en MongoDB")

        return True

    except Exception as e:
        context.log.error(f"❌ Error cargando a MongoDB: {e}")
        raise


# Asset para cargar a PostgreSQL
@asset(group_name="centro", required_resource_keys={"postgres_resource"})
def postgres_data_centro(
    context: AssetExecutionContext,
    clean_data_centro: pd.DataFrame,
):
    """Cargar a PostgreSQL - Depende de clean_data"""
    context.log.info("🗄️ Cargando a PostgreSQL...")

    try:
        # ✅ Usar el recurso de PostgreSQL
        engine = context.resources.postgres_resource

        # ✅ Cargar datos
        clean_data_centro.to_sql(
            "centro_limpios", engine, if_exists="replace", index=False
        )

        # ✅ Verificar que se cargaron los datos
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM centro_limpios")).scalar()

        context.log.info(f"✅ Cargados {count} registros a PostgreSQL")
        return True

    except exc.SQLAlchemyError as e:
        context.log.error(f"❌ Error de base de datos: {e}")
        raise
    except Exception as e:
        context.log.error(f"❌ Error inesperado: {e}")
        raise


# asset para cargar a duckdb
from etl_dagster.assets.ofertas import duckdb_data_ofertas


@asset(group_name="centro", required_resource_keys={"duckdb_resource"}, deps=[duckdb_data_ofertas])
def duckdb_data_centro(
    context: AssetExecutionContext,
    clean_data_centro: pd.DataFrame,
):
    """Cargar a DuckDB - Depende de clean_data"""
    context.log.info("🦆 Cargando a DuckDB...")

    try:
        # ✅ Usar el recurso de DuckDB y obtener una conexión
        with context.resources.duckdb_resource.get_connection() as con:
            context.log.info("✅ Conectado a DuckDB")

            con.register("temp_clean_data", clean_data_centro)
            con.execute(
                "CREATE OR REPLACE TABLE centro_limpios AS SELECT * FROM temp_clean_data"
            )

            result = con.execute("SELECT COUNT(*) FROM ofertas_limpio").fetchone()
            count = result[0] if result is not None else 0
            context.log.info(f"📊 {count} registros insertados")

        return True

    except Exception as e:
        context.log.error(f"❌ Error cargando a DuckDB: {e}")
        raise
