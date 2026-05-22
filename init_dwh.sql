-- =============================================
-- Data Warehouse - Hostel Analytics Pipeline
-- Tablas de staging, dimensiones y KPIs
-- =============================================

-- =============================================
-- STAGING: datos crudos normalizados post-ETL
-- =============================================

-- Staging de hostels (viene de hostel_detail_data.csv)
CREATE TABLE IF NOT EXISTS stg_hostels (
    id_hostel          INTEGER PRIMARY KEY,
    name               TEXT NOT NULL,
    type               VARCHAR(30) NOT NULL,        -- HOSTEL, HOTEL, GUESTHOUSE, APARTMENT, CAMPSITE
    star_rating        INTEGER NOT NULL DEFAULT 0,  -- 0 a 5
    city_name          TEXT NOT NULL,
    city_country       TEXT NOT NULL,
    total_ratings      INTEGER NOT NULL DEFAULT 0,
    free_cancellation  BOOLEAN DEFAULT FALSE,
    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging de usuarios (extraídos de hostel_review_data.csv)
CREATE TABLE IF NOT EXISTS stg_users (
    id_user               BIGINT PRIMARY KEY,
    gender_code           VARCHAR(20),    -- FEMALE, MALE, COUPLE, ALLFEMALEGROUP, MIXEDGROUP, ALLMALEGROUP
    age_range             VARCHAR(10),    -- 18-24, 25-30, 31-40, 41+
    trip_code             VARCHAR(30),    -- REGULARVACATION, RTWTRIP, GAPYEAR, WEEKENDAWAY, OTHER, COLLEGEBREAK
    nationality_code      VARCHAR(10),
    nationality_name      TEXT,
    number_reviews        INTEGER,
    loaded_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging de reseñas (viene de hostel_review_data.csv)
CREATE TABLE IF NOT EXISTS stg_reviews (
    id_review             BIGINT PRIMARY KEY,
    id_hostel             INTEGER NOT NULL REFERENCES stg_hostels(id_hostel),
    id_user               BIGINT NOT NULL,
    review_date           DATE,
    rating_overall        NUMERIC(5,2),
    rating_safety         NUMERIC(5,2),
    rating_location       NUMERIC(5,2),
    rating_staff          NUMERIC(5,2),
    rating_atmosphere     NUMERIC(5,2),
    rating_cleanliness    NUMERIC(5,2),
    rating_facilities     NUMERIC(5,2),
    loaded_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- KPI 1: Cobertura de reseñas
-- (hostels con reseñas / total hostels) × 100
-- =============================================
CREATE TABLE IF NOT EXISTS kpi_coverage (
    id                    SERIAL PRIMARY KEY,
    total_hostels         INTEGER NOT NULL,
    hostels_with_reviews  INTEGER NOT NULL,
    coverage_pct          NUMERIC(5,2) NOT NULL,
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- KPI 2 y 3: Rating promedio por hostel
-- (overall + por dimensión)
-- =============================================
CREATE TABLE IF NOT EXISTS kpi_hostel_ratings (
    id_hostel             INTEGER NOT NULL REFERENCES stg_hostels(id_hostel),
    review_count          INTEGER NOT NULL,
    avg_overall           NUMERIC(6,2),
    avg_safety            NUMERIC(6,2),
    avg_location          NUMERIC(6,2),
    avg_staff             NUMERIC(6,2),
    avg_atmosphere        NUMERIC(6,2),
    avg_cleanliness       NUMERIC(6,2),
    avg_facilities        NUMERIC(6,2),
    stddev_overall        NUMERIC(6,2),   -- KPI 9: consistencia
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id_hostel)
);

-- =============================================
-- KPI 5, 6 y 7: Rankings y desvíos dentro de
-- grupos equivalentes (starRating + type + ciudad/país)
-- =============================================
CREATE TABLE IF NOT EXISTS kpi_equivalent_groups (
    id                    SERIAL PRIMARY KEY,
    group_key             TEXT NOT NULL,           -- 'starRating|type|city_name' o '|city_country'
    group_level           VARCHAR(10) NOT NULL,    -- 'city' o 'country'
    star_rating           INTEGER NOT NULL,
    hostel_type           VARCHAR(30) NOT NULL,
    location              TEXT NOT NULL,           -- city_name o city_country según group_level
    hostel_count          INTEGER NOT NULL,
    review_count          INTEGER NOT NULL,
    avg_overall_group     NUMERIC(6,2) NOT NULL,
    is_valid              BOOLEAN NOT NULL DEFAULT TRUE,  -- FALSE si no alcanza muestra mínima
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(group_key)
);

-- Ranking de cada hostel dentro de su grupo equivalente
CREATE TABLE IF NOT EXISTS kpi_hostel_ranking (
    id_hostel             INTEGER NOT NULL REFERENCES stg_hostels(id_hostel),
    group_key             TEXT NOT NULL REFERENCES kpi_equivalent_groups(group_key),
    avg_overall           NUMERIC(6,2) NOT NULL,
    rank_in_group         INTEGER NOT NULL,
    deviation_from_group  NUMERIC(6,2) NOT NULL,  -- KPI 6: hostel - promedio grupo
    review_count          INTEGER NOT NULL,
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id_hostel)
);

-- KPI 7: correlación volumen de reseñas vs rating promedio, por grupo
CREATE TABLE IF NOT EXISTS kpi_volume_vs_rating (
    group_key             TEXT NOT NULL REFERENCES kpi_equivalent_groups(group_key),
    pearson_r             NUMERIC(5,4),            -- correlación entre count_reviews y avg_overall
    sample_size           INTEGER NOT NULL,
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_key)
);

-- =============================================
-- KPI 4 y 8: Drivers de satisfacción
-- (correlaciones dimensión vs overall)
-- =============================================

-- KPI 4: drivers globales dentro de grupos equivalentes
CREATE TABLE IF NOT EXISTS kpi_global_drivers (
    group_key             TEXT NOT NULL REFERENCES kpi_equivalent_groups(group_key),
    corr_safety           NUMERIC(5,4),
    corr_location         NUMERIC(5,4),
    corr_staff            NUMERIC(5,4),
    corr_atmosphere       NUMERIC(5,4),
    corr_cleanliness      NUMERIC(5,4),
    corr_facilities       NUMERIC(5,4),
    top_driver            VARCHAR(20),             -- dimensión con mayor correlación
    sample_size           INTEGER NOT NULL,
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (group_key)
);

-- KPI 8: drivers por perfil de usuario
-- Segmento = trip_code + gender_code + age_range + hostel_type
CREATE TABLE IF NOT EXISTS kpi_user_profile_drivers (
    id                    SERIAL PRIMARY KEY,
    trip_code             VARCHAR(30),
    gender_code           VARCHAR(20),
    age_range             VARCHAR(10),
    hostel_type           VARCHAR(30),
    segment_key           TEXT NOT NULL,           -- concatenación de los 4 campos
    corr_safety           NUMERIC(5,4),
    corr_location         NUMERIC(5,4),
    corr_staff            NUMERIC(5,4),
    corr_atmosphere       NUMERIC(5,4),
    corr_cleanliness      NUMERIC(5,4),
    corr_facilities       NUMERIC(5,4),
    top_driver            VARCHAR(20),
    sample_size           INTEGER NOT NULL,
    is_valid              BOOLEAN NOT NULL DEFAULT TRUE,  -- FALSE si muestra insuficiente
    calculated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(segment_key)
);

-- =============================================
-- CONTROL: log de ejecuciones del pipeline
-- =============================================
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id                    SERIAL PRIMARY KEY,
    dag_run_id            TEXT NOT NULL,
    stage                 VARCHAR(50) NOT NULL,    -- ingesta, limpieza, transform, kpis, etc.
    status                VARCHAR(20) NOT NULL,    -- success, failed, skipped
    records_processed     INTEGER,
    records_excluded      INTEGER,
    exclusion_reason      TEXT,
    started_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at           TIMESTAMP
);

-- =============================================
-- ÍNDICES para consultas de Metabase
-- =============================================
CREATE INDEX IF NOT EXISTS idx_stg_reviews_hostel   ON stg_reviews(id_hostel);
CREATE INDEX IF NOT EXISTS idx_stg_reviews_user     ON stg_reviews(id_user);
CREATE INDEX IF NOT EXISTS idx_stg_reviews_date     ON stg_reviews(review_date);
CREATE INDEX IF NOT EXISTS idx_stg_hostels_type     ON stg_hostels(type);
CREATE INDEX IF NOT EXISTS idx_stg_hostels_country  ON stg_hostels(city_country);
CREATE INDEX IF NOT EXISTS idx_stg_hostels_star     ON stg_hostels(star_rating);
CREATE INDEX IF NOT EXISTS idx_ranking_group        ON kpi_hostel_ranking(group_key);
CREATE INDEX IF NOT EXISTS idx_profile_drivers_seg  ON kpi_user_profile_drivers(segment_key);
