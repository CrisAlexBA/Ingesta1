import datetime

def obtener_semestre():
    """
    Obtiene el semestre actual en formato 'año-semestre' (e.g., '2024-1').
    Retorna '1' para el primer semestre (enero-junio) y '2' para el segundo semestre (julio-diciembre).
    """
    año_actual = datetime.datetime.now().year
    mes_actual = datetime.datetime.now().month
    semestre = 1 if mes_actual <= 6 else 2
    return f"{año_actual}-{semestre}"

def obtener_facultad_programa(nombre_archivo):
    """
    Obtiene la facultad y el programa académico de un archivo a partir de su nombre.
    Ejemplo de nombre de archivo: 'ciencias_humanas_psicologia.csv'
    """
    facultades = ['ciencias_agroindustriales', 'ciencias_humanas', 'ingenieria', 'medicina', 'economia']
    for facultad in facultades:
        if facultad in nombre_archivo:
            # Extraer el programa después del nombre de la facultad
            programa = nombre_archivo.split(facultad + '_', 1)[1].rsplit('.csv', 1)[0]
            if facultad == 'ingenieria':
                programa = facultad + '_' + programa
            return facultad, programa
    return 'desconocido', 'desconocido'

def limpiar_columnas(df):
    """
    Elimina espacios en blanco al inicio y al final de los nombres de las columnas en un DataFrame de Pandas.
    """
    df.columns = df.columns.str.strip()
