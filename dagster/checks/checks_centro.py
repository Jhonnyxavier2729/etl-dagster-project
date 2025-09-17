from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, AssetIn
import pandas as pd
from assets_csv.centro import raw_data_centro, clean_data_centro, clean_csv_data_centro, mongodb_data_centro, postgres_data_centro, duckdb_data_centro
import os
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import duckdb


@asset_check(asset=raw_data_centro, description="Verificar que el CSV se cargó exitosamente")
def check_csv_centro_cargado_exitosamente(context, raw_data_centro):  

    context.log.info("Ejecutando check para validar carga...")#  Añade context

    passed = isinstance(raw_data_centro, pd.DataFrame) and len(raw_data_centro) > 0
    
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "asset": "raw_data_centro",
            "filas": len(raw_data_centro) if passed else 0,
            "columnas": len(raw_data_centro.columns) if passed else 0,
            "estado": "Carga exitosa" if passed else "Error en carga"
        }
    )


@asset_check(asset=clean_data_centro, description="Verificar que no hay datos vacíos en centro")
def check_centro_no_datos_vacios(context, clean_data_centro):
    """Check: Validar que no existen valores vacíos en centro"""
    vacios_nulos = clean_data_centro.isnull().sum().sum()
    vacios_string = (clean_data_centro == '').sum().sum()
    espacios_simples = (clean_data_centro == ' ').sum().sum()
    espacios_multiples = (clean_data_centro == '   ').sum().sum()
    vacios_nan_string = (clean_data_centro == 'nan').sum().sum()
    
    total_vacios = vacios_nulos + vacios_string + espacios_simples + espacios_multiples+ vacios_nan_string
    passed = total_vacios == 0
    
    return AssetCheckResult(
        passed=bool(passed),
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "asset": "clean_data_centro",
            "total_valores_vacios": int(total_vacios),
            "valores_nulos": int(vacios_nulos),
            "strings_vacios": int(vacios_string),
            "strings_nan": int(vacios_nan_string),
            "total_registros": len(clean_data_centro) * len(clean_data_centro.columns)
        }
    )


@asset_check(asset=clean_data_centro, description="Verificar que todas las columnas son strings en centro")
def check_centro_columnas_string(context, clean_data_centro):  

    """Check: Validar que todas las columnas son de tipo string"""

    tipos_correctos = all(clean_data_centro[col].dtype == 'object' for col in clean_data_centro.columns)
    
    return AssetCheckResult(
        passed=tipos_correctos,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "asset": "clean_data_centro",
            "todos_son_strings": tipos_correctos,
            "tipos_actuales": {col: str(dtype) for col, dtype in clean_data_centro.dtypes.items()}
        }
    )


#validacion de que el archivo csv_limpio se creo correctamente
@asset_check(asset=clean_csv_data_centro, description="Verificar que el CSV se creo correctamente")
def check_centro_csv_creado(context, clean_csv_data_centro):
    """Check: Validar que el archivo CSV limpio se creó correctamente"""

    # El parámetro 'clean_csv_data_centro' CONTIENE la ruta del archivo.
    file_path = clean_csv_data_centro

    try:
        # Verificamos si el archivo existe y no está vacío.
        archivo_existe = os.path.exists(file_path)
        tamano_valido = os.path.getsize(file_path) > 0 if archivo_existe else False
        
        creado_correctamente = archivo_existe and tamano_valido
    except Exception as e:
        context.log.error(f"Error al verificar el archivo CSV desde '{file_path}': {str(e)}")
        creado_correctamente = False
    
    return AssetCheckResult(
        passed=creado_correctamente,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "ruta_verificada": str(file_path),
            "archivo_creado_y_no_vacio": creado_correctamente
        }
    )


# Asegúrar que en la base de datos MongoDB el conteo de filas coincida con los datos del DataFrame limpio
@asset_check(
    asset=mongodb_data_centro, 
    description="Verificar que el conteo de filas coincida con los documentos en MongoDB.",
    additional_ins={"clean_data_centro": AssetIn()})

# 👇 CORRECCIÓN: Añadimos el parámetro del asset asociado
def mongodb_data_check(context, mongodb_data_centro, clean_data_centro: pd.DataFrame):
    """
    Compara el número de filas del DataFrame de entrada con el número
    de documentos en la colección de MongoDB.
    """
    # La variable 'mongodb_data_centro' contiene 'True', no la usamos, 
    # pero debe estar aquí para que la firma sea correcta.
    
    num_dataframe_rows = len(clean_data_centro)
    num_mongo_docs = -1
    
    try:
        mongo_uri = os.environ.get('MONGO_URI')
        if not mongo_uri:
            raise ValueError("MONGO_URI no configurado")
            
        client = MongoClient(mongo_uri, authSource='admin', serverSelectionTimeoutMS=5000)
        db = client['etl_database']
        collection = db['centro_limpios']
        
        num_mongo_docs = collection.count_documents({})
        
        passed = (num_dataframe_rows == num_mongo_docs)

    except Exception as e:
        context.log.error(f"Error en el check de MongoDB: {e}")
        passed = False
    finally:
        if 'client' in locals():
            client.close()

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_esperadas (DataFrame)": num_dataframe_rows,
            "documentos_encontrados (Mongo)": num_mongo_docs,
        }
    )


#check para validar si base de datos postgres fueron cargados correctamente
@asset_check(
    asset=postgres_data_centro, 
    description="Verificar que el conteo de filas coincida con los registros en PostgreSQL.",
    additional_ins={"clean_data_centro": AssetIn()})


def check_postgres_row_count(context, postgres_data_centro, clean_data_centro: pd.DataFrame):
    """
    Compara el número de filas del DataFrame con el número de registros 
    en la tabla de PostgreSQL.
    """
    num_dataframe_rows = len(clean_data_centro)
    num_postgres_rows = -1  # Valor inicial para indicar que no se ha contado
    
    try:
        postgres_uri = os.environ.get('POSTGRES_URI')
        if not postgres_uri:
            raise ValueError("POSTGRES_URI no configurado")
            
        engine = create_engine(postgres_uri)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM centro_limpios"))
            num_postgres_rows = result.scalar_one()

        passed = (num_dataframe_rows == num_postgres_rows)

    except Exception as e:
        context.log.error(f"Error en el check de PostgreSQL: {e}")
        passed = False

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_esperadas (DataFrame)": num_dataframe_rows,
            "registros_encontrados (Postgres)": num_postgres_rows,
        }
    )


#check para validar si base de datos duckdb fueron cargados correctamente
@asset_check(
    asset=duckdb_data_centro, 
    description="Verificar que el conteo de filas coincida con los registros en DuckDB.",
    additional_ins={"clean_data_centro": AssetIn()})


def check_duckdb_row_count(context, duckdb_data_centro, clean_data_centro: pd.DataFrame):
    """
    Compara el número de filas del DataFrame con el número de registros 
    en la tabla de DuckDB.
    """
    num_dataframe_rows = len(clean_data_centro)
    num_duckdb_rows = -1  # Valor inicial para indicar que no se ha contado
    
    try:
        con = duckdb.connect('/duckdb/data.db', read_only=True)
        num_duckdb_rows = con.execute("SELECT COUNT(*) FROM centro_limpios").fetchone()[0]
        con.close()

        passed = (num_dataframe_rows == num_duckdb_rows)

    except Exception as e:
        context.log.error(f"Error en el check de DuckDB: {e}")
        passed = False

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "filas_esperadas (DataFrame)": num_dataframe_rows,
            "registros_encontrados (DuckDB)": num_duckdb_rows,
        }
    )


all_asset_checks = [
    check_csv_centro_cargado_exitosamente,
    check_centro_no_datos_vacios,
    check_centro_columnas_string,
    check_centro_csv_creado,
    mongodb_data_check,
    check_postgres_row_count,
    check_duckdb_row_count
]