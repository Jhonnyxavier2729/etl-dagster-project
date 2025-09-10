from dagster import asset, AssetExecutionContext
import pandas as pd
import duckdb
import os


#Asset para cargar a DuckDB
@asset
def duckdb_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Cargar a DuckDB - Depende de clean_data"""
    context.log.info("🦆 Cargando a DuckDB...")
    try:
        os.makedirs('/data/backup/duckdb', exist_ok=True)
        
        con = duckdb.connect('/duckdb/data.db')
        con.register('temp_data', clean_data)
        con.execute("CREATE OR REPLACE TABLE datos_limpios AS SELECT * FROM temp_data")
        con.close()
        
        return True
        
    except Exception as e:
        context.log.error(f"❌ Error cargando a DuckDB: {e}")
        raise