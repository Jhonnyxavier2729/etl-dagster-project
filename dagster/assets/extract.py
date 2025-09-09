from dagster import asset, AssetExecutionContext
import pandas as pd

@asset
def raw_data(context: AssetExecutionContext):
    """Extraer datos del CSV - Asset inicial"""
    context.log.info("📥 Extrayendo datos del CSV...")
    
    try:
        # Intentar detectar automáticamente el delimitador
        try:
            # Primero intentar con coma
            df = pd.read_csv('/data/REGIONAL_202508131027.csv')
            context.log.info("✅ CSV leído con delimitador: coma (,)")
        except:
            # Si falla, intentar con punto y coma
            df = pd.read_csv('/data/REGIONAL_202508131027.csv', sep=';')
            context.log.info("✅ CSV leído con delimitador: punto y coma (;)")
        
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