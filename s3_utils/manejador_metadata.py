import datetime

def generar_metadata(semestre, facultad, programa):
    return {
        "sem": semestre,
        "area": "academico",
        "fac": facultad,
        "prog": programa,
        "tipo_doc": "notas",
        "subarea": facultad,
        "fecha_creacion": datetime.datetime.now().isoformat(),
        "descripcion": f"notas de estudiantes del programa {programa} para el semestre {semestre}",
        "confidencialidad": "restringido",
        "tipo_archivo": "notas_estudiantes"
    }
