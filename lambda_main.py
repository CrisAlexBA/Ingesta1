import os
from ingesta.ingestaLotes import procesar_lote
from utils.manejador_logs import registrar_error
import boto3

# Cliente S3
s3 = boto3.client('s3')
nombre_bucket = os.getenv('BUCKET_NAME', 'uq-datalake-test')
ruta_prefijo = os.getenv('RUTA_PREFIJO', 'archivos/')

def lambda_handler(event, context):
    archivos = [record['s3']['object']['key'] for record in event['Records']]
    
    # Filtrar archivos por extensión
    archivos_validos = [archivo for archivo in archivos if archivo.endswith(('.pdf', '.docx'))]

    if archivos_validos:
        procesar_lote(nombre_bucket, archivos_validos)
    else:
        print("No se encontraron archivos con extensiones .pdf o .docx.")
