import boto3
from ingesta.ingestaDirecta import procesar_acta
from utils.manejador_logs import registrar_error

# Cliente de S3
s3 = boto3.client('s3')

def procesar_lote(nombre_bucket, archivos, tamaño_lote=5, max_tamaño_mb=100):
    """
    Procesa archivos en lotes desde una lista de archivos en S3.

    Parámetros:
    nombre_bucket (str): 'uq-datalake'
    archivos (list): los archivos que se van a ingresar 
    tamaño_lote (int): 5
    max_tamaño_mb (int): 100
    """
    lote = []
    tamaño_actual = 0

    for archivo_s3 in archivos:
        # Obtener el tamaño del archivo en S3
        obj = s3.head_object(Bucket=nombre_bucket, Key=archivo_s3)
        tamaño_archivo_mb = obj['ContentLength'] / (1024 * 1024)  # Convertir bytes a MB

        # Verificar si el archivo cabe en el lote actual
        if len(lote) < tamaño_lote and (tamaño_actual + tamaño_archivo_mb) <= max_tamaño_mb:
            lote.append(archivo_s3)
            tamaño_actual += tamaño_archivo_mb
        else:
            # Procesar el lote actual cuando alcanza el límite
            procesar_lote_archivos(nombre_bucket, lote)
            lote = [archivo_s3]
            tamaño_actual = tamaño_archivo_mb

    # Procesar el último lote si contiene archivos
    if lote:
        procesar_lote_archivos(nombre_bucket, lote)

def procesar_lote_archivos(nombre_bucket, lote):
    """
    Procesa cada archivo en el lote utilizando la ingesta directa.

    Parámetros:
    nombre_bucket (str): uq-datalake
    lote (list): Lista de archivos en el lote a procesar.
    """
    print(f"Procesando lote de {len(lote)} archivos.")
    for archivo in lote:
        try:
            procesar_acta(nombre_bucket, archivo)  # Reutiliza la función de ingesta directa
        except Exception as e:
            registrar_error(f"Error al procesar el archivo {archivo} en el lote: {e}")
