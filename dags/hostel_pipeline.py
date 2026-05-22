"""
DAG: hostel_pipeline
Pipeline ETL para análisis de hostels y reseñas.
Lee CSVs locales desde /opt/airflow/data/input/,
carga staging en el DWH y calcula KPIs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ── Configuración ──────────────────────────────────────────────────────────────
DATA_DIR = "/opt/airflow/data/input"
DETAIL_FILE = f"{DATA_DIR}/hostel_detail_data.csv"
REVIEW_FILE = f"{DATA_DIR}/hostel_review_data.csv"
CONN_ID = "postgres_dwh"
MIN_REVIEWS_GROUP = 10   # mínimo de reseñas para que un grupo equivalente sea válido
MIN_REVIEWS_SEGMENT = 30 # mínimo para KPI de drivers por perfil de usuario

log = logging.getLogger(__name__)

default_args = {
    "owner": "hostel-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _log_stage(hook, dag_run_id, stage, status, processed=0, excluded=0, reason=None):
    hook.run(
        """
        INSERT INTO pipeline_run_log
            (dag_run_id, stage, status, records_processed, records_excluded, exclusion_reason, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """,
        parameters=(dag_run_id, stage, status, processed, excluded, reason),
    )


# ── Tarea 1: verificar archivos ────────────────────────────────────────────────

def verificar_archivos(**ctx):
    import os
    missing = [f for f in [DETAIL_FILE, REVIEW_FILE] if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(f"Archivos no encontrados: {missing}")
    log.info("Archivos verificados: %s y %s", DETAIL_FILE, REVIEW_FILE)


# ── Tarea 2: cargar staging ────────────────────────────────────────────────────

def cargar_staging(**ctx):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dag_run_id = ctx["run_id"]

    # ── 2a. Hostels ────────────────────────────────────────────────────────────
    log.info("Leyendo hostel_detail_data.csv...")
    df_h = pd.read_csv(DETAIL_FILE, sep=";", on_bad_lines="skip", low_memory=False)

    # Renombrar columnas con punto
    df_h = df_h.rename(columns={
        "city.name": "city_name",
        "city.country": "city_country",
        "freeCancellation.isAvailable": "free_cancellation",
    })

    cols_h = ["id_hostel", "name", "type", "starRating", "city_name",
              "city_country", "totalRatings", "free_cancellation"]
    df_h = df_h[cols_h].copy()
    df_h = df_h.dropna(subset=["id_hostel", "type", "city_name", "city_country"])
    df_h = df_h.drop_duplicates(subset=["id_hostel"])
    df_h["starRating"] = df_h["starRating"].fillna(0).astype(int)
    df_h["totalRatings"] = df_h["totalRatings"].fillna(0).astype(int)
    df_h["free_cancellation"] = df_h["free_cancellation"].fillna(False).astype(bool)

    total_h = len(df_h)
    log.info("Hostels a cargar: %d", total_h)

    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE stg_hostels CASCADE")

    for _, row in df_h.iterrows():
        cur.execute(
            """
            INSERT INTO stg_hostels
                (id_hostel, name, type, star_rating, city_name, city_country,
                 total_ratings, free_cancellation)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_hostel) DO NOTHING
            """,
            (int(row.id_hostel), str(row["name"]), str(row.type),
             int(row.starRating), str(row.city_name), str(row.city_country),
             int(row.totalRatings), bool(row.free_cancellation)),
        )
    conn.commit()
    log.info("stg_hostels cargada: %d filas", total_h)
    _log_stage(hook, dag_run_id, "cargar_staging_hostels", "success", total_h)

    # ── 2b. Reviews y usuarios ─────────────────────────────────────────────────
    log.info("Leyendo hostel_review_data.csv (puede tardar unos segundos)...")
    df_r = pd.read_csv(REVIEW_FILE, sep=";", on_bad_lines="skip", low_memory=False)

    total_raw = len(df_r)
    df_r = df_r.dropna(subset=["id_review", "id_hostel", "id_user"])
    df_r = df_r.drop_duplicates(subset=["id_review"])

    # Solo reviews de hostels que existen en staging
    valid_hostels = set(df_h["id_hostel"].astype(int))
    df_r["id_hostel"] = df_r["id_hostel"].astype(int)
    excluded = len(df_r[~df_r["id_hostel"].isin(valid_hostels)])
    df_r = df_r[df_r["id_hostel"].isin(valid_hostels)]

    log.info("Reviews: %d raw → %d válidas (%d excluidas por hostel no encontrado)",
             total_raw, len(df_r), excluded)

    # Usuarios
    cur.execute("TRUNCATE TABLE stg_users CASCADE")
    df_u = df_r[["id_user", "user_gender_code", "user_age", "user_trip_code",
                  "user_nationality_code", "user_nationality_name",
                  "user_number_reviews"]].drop_duplicates(subset=["id_user"])

    for _, row in df_u.iterrows():
        cur.execute(
            """
            INSERT INTO stg_users
                (id_user, gender_code, age_range, trip_code,
                 nationality_code, nationality_name, number_reviews)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_user) DO NOTHING
            """,
            (int(row.id_user),
             str(row.user_gender_code) if pd.notna(row.user_gender_code) else None,
             str(row.user_age) if pd.notna(row.user_age) else None,
             str(row.user_trip_code) if pd.notna(row.user_trip_code) else None,
             str(row.user_nationality_code) if pd.notna(row.user_nationality_code) else None,
             str(row.user_nationality_name) if pd.notna(row.user_nationality_name) else None,
             int(row.user_number_reviews) if pd.notna(row.user_number_reviews) else None),
        )
    conn.commit()
    log.info("stg_users cargada: %d filas", len(df_u))

    # Reviews
    cur.execute("TRUNCATE TABLE stg_reviews CASCADE")
    rating_cols = ["review_rating_overall", "review_safety_rate", "review_location_rate",
                   "review_staff_rate", "review_atmosphere_rate",
                   "review_cleanliness_rate", "review_facilities_rate"]
    for col in rating_cols:
        df_r[col] = pd.to_numeric(df_r[col], errors="coerce")

    for _, row in df_r.iterrows():
        cur.execute(
            """
            INSERT INTO stg_reviews
                (id_review, id_hostel, id_user, review_date,
                 rating_overall, rating_safety, rating_location, rating_staff,
                 rating_atmosphere, rating_cleanliness, rating_facilities)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_review) DO NOTHING
            """,
            (int(row.id_review), int(row.id_hostel), int(row.id_user),
             row.review_date if pd.notna(row.review_date) else None,
             row.review_rating_overall if pd.notna(row.review_rating_overall) else None,
             row.review_safety_rate if pd.notna(row.review_safety_rate) else None,
             row.review_location_rate if pd.notna(row.review_location_rate) else None,
             row.review_staff_rate if pd.notna(row.review_staff_rate) else None,
             row.review_atmosphere_rate if pd.notna(row.review_atmosphere_rate) else None,
             row.review_cleanliness_rate if pd.notna(row.review_cleanliness_rate) else None,
             row.review_facilities_rate if pd.notna(row.review_facilities_rate) else None),
        )
    conn.commit()
    cur.close()
    log.info("stg_reviews cargada: %d filas", len(df_r))
    _log_stage(hook, dag_run_id, "cargar_staging_reviews", "success",
               len(df_r), excluded, "hostel_no_encontrado_en_detail")


# ── Tarea 3: KPI 1 — cobertura ─────────────────────────────────────────────────

def calcular_kpi_cobertura(**ctx):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run("TRUNCATE TABLE kpi_coverage")
    hook.run(
        """
        INSERT INTO kpi_coverage (total_hostels, hostels_with_reviews, coverage_pct)
        SELECT
            (SELECT COUNT(*) FROM stg_hostels)                        AS total_hostels,
            COUNT(DISTINCT r.id_hostel)                               AS hostels_with_reviews,
            ROUND(COUNT(DISTINCT r.id_hostel) * 100.0
                  / NULLIF((SELECT COUNT(*) FROM stg_hostels), 0), 2) AS coverage_pct
        FROM stg_reviews r
        """
    )
    result = hook.get_first("SELECT total_hostels, hostels_with_reviews, coverage_pct FROM kpi_coverage ORDER BY id DESC LIMIT 1")
    log.info("KPI Cobertura: %d hostels totales, %d con reseñas (%.2f%%)",
             result[0], result[1], result[2])


# ── Tarea 4: KPIs 2, 3 y 9 — ratings por hostel ───────────────────────────────

def calcular_kpi_ratings(**ctx):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run("TRUNCATE TABLE kpi_hostel_ratings")
    hook.run(
        """
        INSERT INTO kpi_hostel_ratings
            (id_hostel, review_count, avg_overall, avg_safety, avg_location,
             avg_staff, avg_atmosphere, avg_cleanliness, avg_facilities, stddev_overall)
        SELECT
            id_hostel,
            COUNT(*)                        AS review_count,
            ROUND(AVG(rating_overall), 2)   AS avg_overall,
            ROUND(AVG(rating_safety), 2)    AS avg_safety,
            ROUND(AVG(rating_location), 2)  AS avg_location,
            ROUND(AVG(rating_staff), 2)     AS avg_staff,
            ROUND(AVG(rating_atmosphere), 2) AS avg_atmosphere,
            ROUND(AVG(rating_cleanliness), 2) AS avg_cleanliness,
            ROUND(AVG(rating_facilities), 2) AS avg_facilities,
            ROUND(STDDEV(rating_overall), 2) AS stddev_overall
        FROM stg_reviews
        WHERE rating_overall IS NOT NULL
        GROUP BY id_hostel
        """
    )
    count = hook.get_first("SELECT COUNT(*) FROM kpi_hostel_ratings")[0]
    log.info("KPI Ratings: calculado para %d hostels", count)


# ── Tarea 5: KPIs 5, 6 y 7 — grupos equivalentes y rankings ───────────────────

def calcular_kpi_grupos(**ctx):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run("TRUNCATE TABLE kpi_volume_vs_rating")
    hook.run("TRUNCATE TABLE kpi_hostel_ranking")
    hook.run("TRUNCATE TABLE kpi_equivalent_groups")

    # Grupos a nivel ciudad
    hook.run(
        f"""
        INSERT INTO kpi_equivalent_groups
            (group_key, group_level, star_rating, hostel_type, location,
             hostel_count, review_count, avg_overall_group, is_valid)
        SELECT
            h.star_rating || '|' || h.type || '|city|' || h.city_name AS group_key,
            'city',
            h.star_rating,
            h.type,
            h.city_name                                 AS location,
            COUNT(DISTINCT h.id_hostel)                 AS hostel_count,
            COUNT(r.id_review)                          AS review_count,
            ROUND(AVG(r.rating_overall), 2)             AS avg_overall_group,
            COUNT(r.id_review) >= {MIN_REVIEWS_GROUP}   AS is_valid
        FROM stg_hostels h
        JOIN stg_reviews r ON h.id_hostel = r.id_hostel
        WHERE r.rating_overall IS NOT NULL
        GROUP BY h.star_rating, h.type, h.city_name
        ON CONFLICT (group_key) DO NOTHING
        """
    )

    # Grupos a nivel país (fallback para ciudades con pocos datos)
    hook.run(
        f"""
        INSERT INTO kpi_equivalent_groups
            (group_key, group_level, star_rating, hostel_type, location,
             hostel_count, review_count, avg_overall_group, is_valid)
        SELECT
            h.star_rating || '|' || h.type || '|country|' || h.city_country AS group_key,
            'country',
            h.star_rating,
            h.type,
            h.city_country                              AS location,
            COUNT(DISTINCT h.id_hostel)                 AS hostel_count,
            COUNT(r.id_review)                          AS review_count,
            ROUND(AVG(r.rating_overall), 2)             AS avg_overall_group,
            COUNT(r.id_review) >= {MIN_REVIEWS_GROUP}   AS is_valid
        FROM stg_hostels h
        JOIN stg_reviews r ON h.id_hostel = r.id_hostel
        WHERE r.rating_overall IS NOT NULL
        GROUP BY h.star_rating, h.type, h.city_country
        ON CONFLICT (group_key) DO NOTHING
        """
    )

    # Ranking de cada hostel dentro de su grupo equivalente (nivel ciudad primero)
    hook.run(
        """
        INSERT INTO kpi_hostel_ranking
            (id_hostel, group_key, avg_overall, rank_in_group,
             deviation_from_group, review_count)
        SELECT
            kr.id_hostel,
            g.group_key,
            kr.avg_overall,
            RANK() OVER (PARTITION BY g.group_key ORDER BY kr.avg_overall DESC) AS rank_in_group,
            ROUND(kr.avg_overall - g.avg_overall_group, 2)  AS deviation_from_group,
            kr.review_count
        FROM kpi_hostel_ratings kr
        JOIN stg_hostels h ON kr.id_hostel = h.id_hostel
        JOIN kpi_equivalent_groups g
            ON g.group_key = h.star_rating || '|' || h.type || '|city|' || h.city_name
        WHERE g.is_valid = TRUE
        ON CONFLICT (id_hostel) DO NOTHING
        """
    )

    # KPI 7: correlación volumen de reseñas vs rating promedio por grupo
    hook.run(
        """
        INSERT INTO kpi_volume_vs_rating (group_key, pearson_r, sample_size)
        SELECT
            g.group_key,
            ROUND(CORR(kr.review_count, kr.avg_overall)::NUMERIC, 4) AS pearson_r,
            COUNT(*)                                                   AS sample_size
        FROM kpi_equivalent_groups g
        JOIN stg_hostels h
            ON g.group_key = h.star_rating || '|' || h.type || '|city|' || h.city_name
        JOIN kpi_hostel_ratings kr ON h.id_hostel = kr.id_hostel
        WHERE g.is_valid = TRUE
        GROUP BY g.group_key
        HAVING COUNT(*) >= 3
        ON CONFLICT (group_key) DO NOTHING
        """
    )

    count_g = hook.get_first("SELECT COUNT(*) FROM kpi_equivalent_groups WHERE is_valid")[0]
    count_r = hook.get_first("SELECT COUNT(*) FROM kpi_hostel_ranking")[0]
    log.info("KPI Grupos: %d grupos válidos, %d hostels rankeados", count_g, count_r)


# ── Tarea 6: KPIs 4 y 8 — drivers de satisfacción ─────────────────────────────

def calcular_kpi_drivers(**ctx):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run("TRUNCATE TABLE kpi_global_drivers")
    hook.run("TRUNCATE TABLE kpi_user_profile_drivers")

    # KPI 4: drivers globales por grupo equivalente (nivel ciudad)
    hook.run(
        f"""
        INSERT INTO kpi_global_drivers
            (group_key, corr_safety, corr_location, corr_staff,
             corr_atmosphere, corr_cleanliness, corr_facilities,
             top_driver, sample_size)
        SELECT
            g.group_key,
            ROUND(CORR(r.rating_safety,      r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_location,    r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_staff,       r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_atmosphere,  r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_cleanliness, r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_facilities,  r.rating_overall)::NUMERIC, 4),
            -- dimensión con mayor correlación absoluta
            (ARRAY['safety','location','staff','atmosphere','cleanliness','facilities'])[
                array_position(ARRAY[
                    ABS(CORR(r.rating_safety,      r.rating_overall)),
                    ABS(CORR(r.rating_location,    r.rating_overall)),
                    ABS(CORR(r.rating_staff,       r.rating_overall)),
                    ABS(CORR(r.rating_atmosphere,  r.rating_overall)),
                    ABS(CORR(r.rating_cleanliness, r.rating_overall)),
                    ABS(CORR(r.rating_facilities,  r.rating_overall))
                ],
                GREATEST(
                    ABS(CORR(r.rating_safety,      r.rating_overall)),
                    ABS(CORR(r.rating_location,    r.rating_overall)),
                    ABS(CORR(r.rating_staff,       r.rating_overall)),
                    ABS(CORR(r.rating_atmosphere,  r.rating_overall)),
                    ABS(CORR(r.rating_cleanliness, r.rating_overall)),
                    ABS(CORR(r.rating_facilities,  r.rating_overall))
                ))
            ],
            COUNT(*) AS sample_size
        FROM kpi_equivalent_groups g
        JOIN stg_hostels h
            ON g.group_key = h.star_rating || '|' || h.type || '|city|' || h.city_name
        JOIN stg_reviews r ON h.id_hostel = r.id_hostel
        WHERE g.is_valid = TRUE
          AND r.rating_overall IS NOT NULL
        GROUP BY g.group_key
        HAVING COUNT(*) >= {MIN_REVIEWS_GROUP}
        ON CONFLICT (group_key) DO NOTHING
        """
    )

    # KPI 8: drivers por perfil de usuario
    hook.run(
        f"""
        INSERT INTO kpi_user_profile_drivers
            (trip_code, gender_code, age_range, hostel_type, segment_key,
             corr_safety, corr_location, corr_staff, corr_atmosphere,
             corr_cleanliness, corr_facilities, top_driver,
             sample_size, is_valid)
        SELECT
            u.trip_code,
            u.gender_code,
            u.age_range,
            h.type                                        AS hostel_type,
            u.trip_code || '|' || u.gender_code || '|'
                || u.age_range || '|' || h.type           AS segment_key,
            ROUND(CORR(r.rating_safety,      r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_location,    r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_staff,       r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_atmosphere,  r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_cleanliness, r.rating_overall)::NUMERIC, 4),
            ROUND(CORR(r.rating_facilities,  r.rating_overall)::NUMERIC, 4),
            (ARRAY['safety','location','staff','atmosphere','cleanliness','facilities'])[
                array_position(ARRAY[
                    ABS(CORR(r.rating_safety,      r.rating_overall)),
                    ABS(CORR(r.rating_location,    r.rating_overall)),
                    ABS(CORR(r.rating_staff,       r.rating_overall)),
                    ABS(CORR(r.rating_atmosphere,  r.rating_overall)),
                    ABS(CORR(r.rating_cleanliness, r.rating_overall)),
                    ABS(CORR(r.rating_facilities,  r.rating_overall))
                ],
                GREATEST(
                    ABS(CORR(r.rating_safety,      r.rating_overall)),
                    ABS(CORR(r.rating_location,    r.rating_overall)),
                    ABS(CORR(r.rating_staff,       r.rating_overall)),
                    ABS(CORR(r.rating_atmosphere,  r.rating_overall)),
                    ABS(CORR(r.rating_cleanliness, r.rating_overall)),
                    ABS(CORR(r.rating_facilities,  r.rating_overall))
                ))
            ],
            COUNT(*)                                      AS sample_size,
            COUNT(*) >= {MIN_REVIEWS_SEGMENT}             AS is_valid
        FROM stg_reviews r
        JOIN stg_users u   ON r.id_user   = u.id_user
        JOIN stg_hostels h ON r.id_hostel = h.id_hostel
        WHERE r.rating_overall IS NOT NULL
          AND u.trip_code   IS NOT NULL
          AND u.gender_code IS NOT NULL
          AND u.age_range   IS NOT NULL
        GROUP BY u.trip_code, u.gender_code, u.age_range, h.type
        ON CONFLICT (segment_key) DO NOTHING
        """
    )

    count_d = hook.get_first("SELECT COUNT(*) FROM kpi_global_drivers")[0]
    count_p = hook.get_first("SELECT COUNT(*) FROM kpi_user_profile_drivers WHERE is_valid")[0]
    log.info("KPI Drivers: %d grupos con driver global, %d segmentos de usuario válidos",
             count_d, count_p)


# ── Definición del DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="hostel_pipeline",
    description="Pipeline ETL: hostels y reviews → DWH → KPIs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # se ejecuta manualmente; cambiar a '@daily' si se quiere programar
    catchup=False,
    default_args=default_args,
    tags=["hostels", "etl", "kpis"],
) as dag:

    t1 = PythonOperator(
        task_id="verificar_archivos",
        python_callable=verificar_archivos,
    )

    t2 = PythonOperator(
        task_id="cargar_staging",
        python_callable=cargar_staging,
    )

    t3 = PythonOperator(
        task_id="calcular_kpi_cobertura",
        python_callable=calcular_kpi_cobertura,
    )

    t4 = PythonOperator(
        task_id="calcular_kpi_ratings",
        python_callable=calcular_kpi_ratings,
    )

    t5 = PythonOperator(
        task_id="calcular_kpi_grupos_y_rankings",
        python_callable=calcular_kpi_grupos,
    )

    t6 = PythonOperator(
        task_id="calcular_kpi_drivers",
        python_callable=calcular_kpi_drivers,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
