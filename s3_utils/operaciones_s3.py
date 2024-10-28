import boto3
from s3_utils.manejador_metadata import generar_metadata

s3 = boto3.client('s3')

def cargar_archivo_s3(nombre_bucket, contenido_csv, ruta_s3, metadata):
    try:
        s3.put_object(Body=contenido_csv.encode('utf-8'), Bucket=nombre_bucket, Key=ruta_s3, Metadata=metadata)
        print(f"Archivo cargado exitosamente a {ruta_s3}")
    except Exception as e:
        print(f"Error al cargar el archivo a {ruta_s3}: {e}")

def eliminar_archivo_s3(nombre_bucket, nombre_archivo_s3):
    try:
        s3.delete_object(Bucket=nombre_bucket, Key=nombre_archivo_s3)
        print(f"Archivo {nombre_archivo_s3} eliminado correctamente.")
    except Exception as e:
        print(f"Error al eliminar el archivo {nombre_archivo_s3}: {e}")
