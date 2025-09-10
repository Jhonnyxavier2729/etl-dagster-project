from dagster import asset, AssetExecutionContext
import pandas as pd

#asset para transformar y limpiar datos
@asset
def clean_data(context: AssetExecutionContext, raw_data: pd.DataFrame):
    """Transformar y limpiar datos - Depende de raw_data"""
    context.log.info("🔄 Iniciando transformación de datos...")
    
    try:
        # Hacer copia del DataFrame original
        df_clean = raw_data.copy()
        
        context.log.info(f"📥 Datos recibidos: {df_clean.shape[0]} filas x {df_clean.shape[1]} columnas")
        
        # ❌ 1. Convertir todas las columnas a string
        context.log.info("🔄 Convirtiendo todas las columnas a string...")
        df_clean = df_clean.astype(str)
        context.log.info(f"Tipos de columnas: {df_clean.dtypes.to_dict()}")



        # 2. ✅ Ahora reemplazar TODOS los valores "vacíos"
        context.log.info("🔄 Reemplazando valores vacíos por 'UKNOWN'...")
        df_clean = df_clean.replace({
            'nan': 'UKNOWN',      # NaN convertidos a string
            'None': 'UKNOWN',     # None convertidos a string  
            '': 'UKNOWN',         # Strings vacíos
            ' ': 'UKNOWN',        # Espacios en blanco
            '  ': 'UKNOWN'        # Múltiples espacios
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