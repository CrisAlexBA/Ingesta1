import datetime

def obtener_semestre():
    """
    Obtiene el semestre actual en formato 'año-semestre' (por ejemplo, '2024-1').
    """
    año_actual = datetime.datetime.now().year
    mes_actual = datetime.datetime.now().month
    semestre = 1 if mes_actual <= 6 else 2
    return f"{año_actual}-{semestre}"

def obtener_facultad_programa(nombre_archivo):
    """
    Extrae la facultad y el programa académico del nombre del archivo.
    
    Parámetros:
    nombre_archivo (str): El nombre del archivo, debe incluir la facultad y programa, separados por '_'.
    
    Retorna:
    tuple: (facultad, programa)
    """
    facultades = ['ciencias_agroindustriales', 'ciencias_humanas', 'ingenieria', 'medicina', 'economia']
    for facultad in facultades:
        if facultad in nombre_archivo:
            # Extrae el programa después del nombre de la facultad
            programa = nombre_archivo.split(facultad + '_', 1)[1].rsplit('.csv', 1)[0]
            if facultad == 'ingenieria':
                programa = facultad + '_' + programa
            return facultad, programa
    return 'desconocido', 'desconocido'

def limpiar_columnas(df):
    """
    Limpia los nombres de las columnas en un DataFrame de Pandas eliminando espacios en blanco.
    
    Parámetros:
    df (DataFrame): DataFrame de pandas cuyos nombres de columna deben limpiarse.
    """
    df.columns = df.columns.str.strip()
