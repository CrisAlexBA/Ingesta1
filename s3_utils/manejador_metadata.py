import datetime

def generar_metadata(semestre, grupo):
    return {
        "sem": semestre,
        "area": "investigaciones",
        "grupo": grupo,
        "tipo_doc": "informe_investigativo",
        "fecha_creacion": datetime.datetime.now().isoformat(),
        "descripcion": f"informe del grupo de investigacion {grupo} para el semestre {semestre}",
        "confidencialidad": "restringido",
        "tipo_archivo": "informe_investigativo"
    }
