"""
Tarea de descarga desde Google Drive usando gdown.
No requiere autenticación ni service account.
Solo necesita que la carpeta de Drive sea pública.

Cómo hacer la carpeta pública:
- Click derecho en la carpeta → Compartir
- Cambiar a "Cualquier persona con el enlace" → Viewer
- Tu compañero necesita acceso Editor para subir archivos

Lógica de detección:
- Descarga todos los CSVs que encuentre en la carpeta
- El más pesado = hostel_review_data.csv (~367MB)
- El más liviano = hostel_detail_data.csv (~189MB)
"""

import os
import logging
import gdown

log = logging.getLogger(__name__)

# ID de la carpeta de Google Drive (no cambia nunca)
FOLDER_ID = "15wdCwnm4mQuwGRGRZ9G5uf3sqopcM5ch"

DESTINO_DETAIL = "/opt/airflow/data/input/hostel_detail_data.csv"
DESTINO_REVIEW = "/opt/airflow/data/input/hostel_review_data.csv"
TEMP_DIR       = "/opt/airflow/data/input/tmp_drive"


def descargar_csvs_drive(**ctx):
    """
    Tarea de Airflow: lista la carpeta pública de Drive,
    descarga todos los CSVs encontrados y los renombra
    según su tamaño (el más grande = reviews, el más chico = detail).
    """
    import shutil

    # Crear carpeta temporal de descarga
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DESTINO_DETAIL), exist_ok=True)

    # Descargar todos los archivos de la carpeta
    log.info("Listando y descargando CSVs desde carpeta de Drive: %s", FOLDER_ID)
    folder_url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"

    gdown.download_folder(
        url=folder_url,
        output=TEMP_DIR,
        quiet=False,
        use_cookies=False,
    )

    # Buscar todos los CSVs descargados
    csvs = [
        os.path.join(TEMP_DIR, f)
        for f in os.listdir(TEMP_DIR)
        if f.lower().endswith(".csv")
    ]

    if len(csvs) < 2:
        raise ValueError(
            f"Se esperaban 2 archivos CSV en la carpeta de Drive pero se encontraron {len(csvs)}. "
            f"Verificar que ambos archivos están en la carpeta compartida."
        )

    if len(csvs) > 2:
        log.warning(
            "Se encontraron %d CSVs en la carpeta, se usarán los 2 más grandes.", len(csvs)
        )
        csvs = sorted(csvs, key=os.path.getsize, reverse=True)[:2]

    # Ordenar por tamaño: el más grande es reviews, el más chico es detail
    csvs_sorted = sorted(csvs, key=os.path.getsize, reverse=True)
    archivo_reviews = csvs_sorted[0]
    archivo_detail  = csvs_sorted[1]

    log.info(
        "Archivo reviews detectado: %s (%.1f MB)",
        os.path.basename(archivo_reviews),
        os.path.getsize(archivo_reviews) / 1024 / 1024,
    )
    log.info(
        "Archivo detail detectado: %s (%.1f MB)",
        os.path.basename(archivo_detail),
        os.path.getsize(archivo_detail) / 1024 / 1024,
    )

    # Mover y renombrar a los nombres esperados por el pipeline
    shutil.move(archivo_reviews, DESTINO_REVIEW)
    shutil.move(archivo_detail,  DESTINO_DETAIL)

    # Limpiar carpeta temporal
    shutil.rmtree(TEMP_DIR, ignore_errors=True)

    log.info("CSVs listos en /opt/airflow/data/input/")
