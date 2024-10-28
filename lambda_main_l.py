import os
import boto3
from ingesta.ingestaLotes import procesar_lote
from utils.manejador_logs import registrar_error

# Configuración del cliente S3 y variables de entorno
s3 = boto3.client('s3')
nombre_bucket = os.getenv('BUCKET_NAME', 'uq-datalake')
ruta_prefijo = os.getenv('RUTA_PREFIJO', 'archivos/')

# Parámetros de tamaño de lote
TAMANO_LOTE = int(os.getenv('TAMANO_LOTE', 5))         # Cantidad de archivos por lote
TAMANO_MAX_MB = int(os.getenv('TAMANO_MAX_MB', 100))   # Tamaño máximo en MB de cada lote

def lambda_handler(event, context):
    """
    Función de entrada de Lambda que procesa archivos en lotes cuando se cargan en S3.
    
    Parámetros:
    event: Diccionario que contiene la información del evento S3.
    context: Información de contexto de la ejecución de Lambda.
    """
    try:
        # Listar archivos en la ruta especificada del bucket
        archivos = listar_archivos_en_ruta(nombre_bucket, ruta_prefijo)
        
        # Ejecutar el procesamiento en lotes
        procesar_lote(nombre_bucket, archivos, TAMANO_LOTE, TAMANO_MAX_MB)
        
    except Exception as e:
        # Registrar el error en caso de falla
        registrar_error(f"Error durante la ingesta por lotes: {e}")
        print(f"Error en la ingesta por lotes: {e}")

def listar_archivos_en_ruta(nombre_bucket, ruta_prefijo):
    """
    Lista los archivos en una ruta específica dentro de un bucket de S3.

    Parámetros:
    nombre_bucket (str): El nombre del bucket en S3.
    ruta_prefijo (str): El prefijo o ruta dentro del bucket donde se buscarán los archivos.

    Retorna:
    list: Lista de archivos (keys) encontrados en la ruta especificada.
    """
    try:
        response = s3.list_objects_v2(Bucket=nombre_bucket, Prefix=ruta_prefijo)
        if 'Contents' in response:
            archivos = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.csv')]
            return archivos
        else:
            print(f"No se encontraron archivos en la ruta {ruta_prefijo}")
            return []
    except Exception as e:
        registrar_error(f"Error al listar los archivos en S3: {e}")
        print(f"Error al listar los archivos en S3: {e}")
        return []
