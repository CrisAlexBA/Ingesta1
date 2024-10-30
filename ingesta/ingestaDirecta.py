import pandas as pd
from io import StringIO
from s3_utils.operaciones_s3 import cargar_archivo_s3
from utils.validaciones_datos import validar_datos
from utils.funciones_auxiliares import obtener_semestre, obtener_facultad_programa, limpiar_columnas
from s3_utils.manejador_metadata import generar_metadata
from utils.manejador_logs import registrar_error
import boto3

# Cliente de S3
s3 = boto3.client('s3')

def procesar_acta(nombre_bucket, nombre_archivo_s3):
    """
    Descarga, valida y sube el archivo de actas de notas al Data Lake en S3.
    """
    try:
        # Descargar el archivo desde S3
        obj = s3.get_object(Bucket=nombre_bucket, Key=nombre_archivo_s3)
        df = pd.read_csv(obj['Body'], encoding='utf-8')
    except Exception as e:
        registrar_error(f"Error al leer el archivo {nombre_archivo_s3}: {e}")
        return

    # Limpiar nombres de columnas
    limpiar_columnas(df)

    # Validar datos
    if validar_datos(df, nombre_archivo_s3):
        semestre = obtener_semestre()
        facultad, programa = obtener_facultad_programa(nombre_archivo_s3)
        ruta_s3 = f"actas_notas/periodo={semestre}/programa={programa}/acta_notas_{programa}.csv"
        metadata = generar_metadata(semestre, facultad, programa)

        # Convertir DataFrame a CSV en memoria
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Cargar archivo procesado a S3
        cargar_archivo_s3(nombre_bucket, csv_buffer.getvalue(), ruta_s3, metadata)
    else:
        print(f"Errores en el archivo {nombre_archivo_s3}. Revise el log.")
