-- =============================================
-- KPIs DE MARKETING Y SEGMENTACIÓN
-- Para dashboard gerencial en Metabase
-- Todas las queries son agrupadas — sin detalle
-- por hostel individual.
-- =============================================

-- ─────────────────────────────────────────────
-- MKT-1: Distribución de hostels por banda
-- de calidad (rating promedio overall)
-- Visualización: torta
-- ─────────────────────────────────────────────
SELECT
    CASE
        WHEN avg_overall >= 90 THEN '⭐ Excelente (90-100)'
        WHEN avg_overall >= 80 THEN '✅ Muy bueno (80-89)'
        WHEN avg_overall >= 70 THEN '👍 Bueno (70-79)'
        WHEN avg_overall >= 60 THEN '⚠️ Regular (60-69)'
        ELSE '❌ Por debajo (< 60)'
    END                                     AS "Banda de Calidad",
    COUNT(*)                                AS "Cantidad de Hostels",
    ROUND(AVG(avg_overall), 1)              AS "Rating Promedio de la Banda",
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS "% del Total"
FROM kpi_hostel_ratings
GROUP BY 1
ORDER BY MIN(avg_overall) DESC;


-- ─────────────────────────────────────────────
-- MKT-2: Hostels sin reseñas
-- Visualización: número/scalar
-- ─────────────────────────────────────────────
SELECT
    hostels_without_reviews
FROM kpi_coverage
ORDER BY id DESC;


-- ─────────────────────────────────────────────
-- MKT-3: Volumen de reseñas por perfil
-- de viajero (tipo de viaje + género)
-- Visualización: barras
-- ─────────────────────────────────────────────
SELECT
    u.trip_code                             AS "Tipo de Viaje",
    u.gender_code                           AS "Género",
    COUNT(r.id_review)                      AS "Total Reseñas",
    ROUND(AVG(r.rating_overall), 1)         AS "Rating Promedio",
    ROUND(COUNT(r.id_review) * 100.0
          / SUM(COUNT(r.id_review)) OVER (), 1) AS "% del Total de Reseñas"
FROM stg_reviews r
JOIN stg_users u ON r.id_user = u.id_user
WHERE u.trip_code IS NOT NULL
  AND u.gender_code IS NOT NULL
  AND r.rating_overall IS NOT NULL
GROUP BY u.trip_code, u.gender_code
ORDER BY COUNT(r.id_review) DESC;


-- ─────────────────────────────────────────────
-- MKT-4: Satisfacción por rango etario
-- y tipo de viaje
-- → 41+ deja mejores ratings — target premium
-- Visualización: tabla con color por rating
-- ─────────────────────────────────────────────
SELECT
    u.age_range                             AS "Rango Etario",
    u.trip_code                             AS "Tipo de Viaje",
    COUNT(r.id_review)                      AS "Total Reseñas",
    ROUND(AVG(r.rating_overall), 1)         AS "Rating Promedio Overall",
    ROUND(AVG(r.rating_cleanliness), 1)     AS "Limpieza Promedio",
    ROUND(AVG(r.rating_atmosphere), 1)      AS "Atmósfera Promedio",
    ROUND(AVG(r.rating_staff), 1)           AS "Staff Promedio"
FROM stg_reviews r
JOIN stg_users u ON r.id_user = u.id_user
WHERE u.age_range IS NOT NULL
  AND u.trip_code IS NOT NULL
  AND r.rating_overall IS NOT NULL
GROUP BY u.age_range, u.trip_code
HAVING COUNT(r.id_review) >= 100
ORDER BY AVG(r.rating_overall) DESC;


-- ─────────────────────────────────────────────
-- MKT-5: Top países por volumen y satisfacción
-- → Mercados con más actividad y mejor experiencia
-- Visualización: tabla
-- ─────────────────────────────────────────────
SELECT
    h.city_country                          AS "País",
    COUNT(DISTINCT h.id_hostel)             AS "Hostels Registrados",
    COUNT(DISTINCT CASE WHEN kr.id_hostel IS NOT NULL
                   THEN h.id_hostel END)    AS "Hostels con Reseñas",
    COUNT(r.id_review)                      AS "Total Reseñas",
    ROUND(AVG(r.rating_overall), 1)         AS "Rating Promedio País",
    ROUND(COUNT(r.id_review) * 1.0
          / NULLIF(COUNT(DISTINCT h.id_hostel), 0), 0) AS "Reseñas por Hostel"
FROM stg_hostels h
LEFT JOIN stg_reviews r ON h.id_hostel = r.id_hostel
LEFT JOIN kpi_hostel_ratings kr ON h.id_hostel = kr.id_hostel
WHERE r.rating_overall IS NOT NULL
GROUP BY h.city_country
HAVING COUNT(r.id_review) >= 500
ORDER BY AVG(r.rating_overall) DESC
LIMIT 20;


-- ─────────────────────────────────────────────
-- MKT-6: Tipo de alojamiento x rango etario
-- → Qué producto prefiere cada edad
-- Visualización: barras agrupadas
-- ─────────────────────────────────────────────
SELECT
    u.age_range                             AS "Rango Etario",
    h.type                                  AS "Tipo de Alojamiento",
    COUNT(r.id_review)                      AS "Total Reseñas",
    ROUND(AVG(r.rating_overall), 1)         AS "Rating Promedio",
    ROUND(COUNT(r.id_review) * 100.0
          / SUM(COUNT(r.id_review)) OVER (PARTITION BY u.age_range), 1)
                                            AS "% dentro del Rango Etario"
FROM stg_reviews r
JOIN stg_users u ON r.id_user = u.id_user
JOIN stg_hostels h ON r.id_hostel = h.id_hostel
WHERE u.age_range IS NOT NULL
  AND r.rating_overall IS NOT NULL
GROUP BY u.age_range, h.type
HAVING COUNT(r.id_review) >= 100
ORDER BY u.age_range, COUNT(r.id_review) DESC;


-- ─────────────────────────────────────────────
-- MKT-7: Nacionalidades con mayor volumen
-- y mejor rating
-- → Targeting geográfico de ads
-- Visualización: tabla top 20
-- ─────────────────────────────────────────────
SELECT
    u.nationality_name                      AS "Nacionalidad",
    COUNT(r.id_review)                      AS "Total Reseñas",
    COUNT(DISTINCT r.id_user)               AS "Usuarios Únicos",
    ROUND(AVG(r.rating_overall), 1)         AS "Rating Promedio",
    ROUND(AVG(r.rating_cleanliness), 1)     AS "Limpieza Promedio",
    ROUND(AVG(r.rating_atmosphere), 1)      AS "Atmósfera Promedio"
FROM stg_reviews r
JOIN stg_users u ON r.id_user = u.id_user
WHERE u.nationality_name IS NOT NULL
  AND r.rating_overall IS NOT NULL
GROUP BY u.nationality_name
HAVING COUNT(r.id_review) >= 200
ORDER BY COUNT(r.id_review) DESC
LIMIT 20;


-- ─────────────────────────────────────────────
-- MKT-8: Evolución de reseñas por año
-- → Tendencia de actividad en la plataforma
-- Visualización: línea temporal
-- Nota: 2024 es año parcial (el dataset se
-- exportó a mitad de año)
-- ─────────────────────────────────────────────
SELECT
    EXTRACT(YEAR FROM review_date)          AS "Año",
    COUNT(*)                                AS "Total Reseñas",
    ROUND(AVG(rating_overall), 1)           AS "Rating Promedio",
    COUNT(DISTINCT id_hostel)               AS "Hostels con Actividad"
FROM stg_reviews
WHERE review_date IS NOT NULL
  AND rating_overall IS NOT NULL
  AND EXTRACT(YEAR FROM review_date) BETWEEN 2010 AND 2024
GROUP BY 1
ORDER BY 1;
