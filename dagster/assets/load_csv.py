from dagster import asset, AssetExecutionContext
import pandas as pd
import os

@asset
def clean_csv_data(context: AssetExecutionContext, clean_data: pd.DataFrame):
    """Guardar CSV limpio - Depende de clean_data"""
    context.log.info("💾 Guardando CSV limpio...")
    try:
        output_path = '/data/output/REGIONAL_202508131027_limpio.csv'
        clean_data.to_csv(output_path, index=False, encoding='utf-8')
        context.log.info(f"✅ CSV guardado en: {output_path}")
        return output_path
    except Exception as e:
        context.log.error(f"❌ Error guardando CSV: {e}")
        raise