import os
from ingesta.ingestaDirecta import procesar_acta
from ingesta.ingestaLotes import procesar_lote_archivos
from utils.manejador_logs import registrar_error
import boto3

# Cliente S3
s3 = boto3.client('s3')
nombre_bucket = os.getenv('BUCKET_NAME', 'uq-datalake')
ruta_prefijo = os.getenv('RUTA_PREFIJO', 'archivos/')

def lambda_handler(event, context):
    for record in event['Records']:
        archivo = record['s3']['object']['key']
        if archivo.endswith('.csv'):
            procesar_acta(nombre_bucket, archivo)
        elif archivo.endswith(('.pdf', '.docx')):
            procesar_lote_archivos(nombre_bucket, [archivo])
