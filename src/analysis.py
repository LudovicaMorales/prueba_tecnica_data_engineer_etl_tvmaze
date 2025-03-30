import logging
import pandas as pd
from ydata_profiling import ProfileReport

logger = logging.getLogger(__name__)

def generate_profiling_report(df: pd.DataFrame, output_file: str):
    """
    Genera un reporte de profiling usando ydata_profiling y lo guarda en formato HTML.
    """
    if df.empty:
        logger.warning("DataFrame vacío. No se puede generar el reporte de profiling.")
        return
    profile = ProfileReport(df, title="TV Shows Profiling Report", explorative=True)
    profile.to_file(output_file)
    logger.info(f"Reporte de profiling generado: {output_file}")