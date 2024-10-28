import os
import boto3
from ingesta.ingestaDirecta import procesar_acta
from utils.manejador_logs import registrar_error

# Cliente de S3
s3 = boto3.client('s3')

# Obtener el nombre del bucket y la ruta de destino desde las variables de entorno de Lambda
nombre_bucket = os.getenv('BUCKET_NAME', 'uq-datalake')  # Puedes cambiar el valor por defecto
ruta_prefijo = os.getenv('RUTA_PREFIJO', 'archivos/')

def lambda_handler(event, context):
    """
    Función de entrada de Lambda que se activa cuando se carga un nuevo archivo en S3.
    
    Parámetros:
    event: Diccionario que contiene la información del evento S3.
    context: Información de contexto de la ejecución de Lambda.
    """
    # Procesar cada archivo cargado
    for record in event['Records']:
        # Obtener el nombre del archivo cargado desde el evento S3
        nombre_archivo_s3 = record['s3']['object']['key']
        
        try:
            # Procesar el archivo usando la función de ingesta directa
            print(f"Procesando archivo cargado: {nombre_archivo_s3}")
            procesar_acta(nombre_bucket, nombre_archivo_s3)
        except Exception as e:
            # Registrar el error en caso de falla
            registrar_error(f"Error al procesar el archivo {nombre_archivo_s3}: {e}")
            print(f"Error procesando el archivo {nombre_archivo_s3}: {e}")
