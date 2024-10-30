from utils.manejador_logs import registrar_error
import boto3

s3 = boto3.client('s3')

def procesar_lote(nombre_bucket, archivos, tamaño_lote=5, max_tamaño_mb=100):
    """
    Procesa archivos de investigación en lotes de tamaño configurado.
    """
    lote = []
    tamaño_actual = 0

    for archivo_s3 in archivos:
        obj = s3.head_object(Bucket=nombre_bucket, Key=archivo_s3)
        tamaño_archivo_mb = obj['ContentLength'] / (1024 * 1024)

        if len(lote) < tamaño_lote and (tamaño_actual + tamaño_archivo_mb) <= max_tamaño_mb:
            lote.append(archivo_s3)
            tamaño_actual += tamaño_archivo_mb
        else:
            procesar_lote_archivos(nombre_bucket, lote)
            lote = [archivo_s3]
            tamaño_actual = tamaño_archivo_mb

    if lote:
        procesar_lote_archivos(nombre_bucket, lote)

def procesar_lote_archivos(nombre_bucket, lote):
    """
    Procesa cada archivo en el lote, trasladándolo a su ubicación en el Data Lake
    según la estructura de almacenamiento para investigaciones.
    """
    for archivo in lote:
        try:
            # Obtener el nombre del archivo y el grupo de la ruta
            nombre_archivo = archivo.split('/')[-1]
            grupo = archivo.split('/')[1]  # 'grupo' es la segunda parte de la ruta en S3

            # Generar la ruta de destino en el Data Lake
            ruta_destino = f"investigaciones/{grupo}/{nombre_archivo}"

            # Copiar el archivo al destino en el Data Lake
            copy_source = {'Bucket': nombre_bucket, 'Key': archivo}
            s3.copy_object(CopySource=copy_source, Bucket=nombre_bucket, Key=ruta_destino)
            print(f"Archivo {nombre_archivo} procesado y almacenado en {ruta_destino}")

        except Exception as e:
            registrar_error(f"Error al procesar el archivo {archivo}: {e}")
            print(f"Error al procesar el archivo {archivo}: {e}")
