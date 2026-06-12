# Hostel Analytics Pipeline

Sistema de procesamiento y visualización de datos para análisis de hostels y reseñas.

**Stack:** Apache Airflow · PostgreSQL · Metabase · Docker

---

## Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Al menos **6 GB de RAM** asignados a Docker
  - Docker Desktop → Settings → Resources → Memory → 6 GB → Apply & Restart

---

## Estructura del proyecto

```
proyecto_hostels/
├── dags/
│   └── hostel_pipeline.py      # DAG principal de Airflow
├── data/
│   └── input/
│       ├── hostel_detail_data.csv
│       └── hostel_review_data.csv
├── logs/                       # se crea automáticamente
├── secrets/                    # credenciales Google Drive (no subir al repo)
├── docker-compose.yml
├── Dockerfile.airflow
└── init_dwh.sql
```

---

## Instalación paso a paso

### 1. Clonar o descomprimir el proyecto

```bash
cd ~/Documentos
# si tienen el zip:
unzip proyecto_hostels.zip
cd proyecto_hostels
```

### 2. Agregar los datasets

Copiar los archivos CSV dentro de `data/input/`:

```
data/input/hostel_detail_data.csv
data/input/hostel_review_data.csv
```

### 3. Crear carpetas necesarias

```bash
mkdir -p logs secrets dags
```

### 4. Levantar todos los servicios

```bash
docker compose up -d --build
```

La primera vez tarda entre 5 y 10 minutos porque descarga y construye las imágenes. Las siguientes veces es mucho más rápido.

### 5. Verificar que todo levantó bien

```bash
docker compose ps
```

Todos los servicios deben estar en `Up` o `healthy`. El `airflow-init` aparece como `Exited (0)` — eso es correcto.

---

## Configuración de Airflow

### Abrir la UI

Navegador → **http://localhost:8080**
- Usuario: `admin`
- Contraseña: `admin`

### Crear la conexión al DWH

1. Ir a **Admin → Connections → +**
2. Completar:

| Campo           | Valor         |
|-----------------|---------------|
| Connection Id   | `postgres_dwh` |
| Connection Type | `Postgres`    |
| Host            | `postgres-dwh` |
| Database        | `dwh`         |
| Login           | `dwh`         |
| Password        | `dwh123`      |
| Port            | `5432`        |

3. Click en **Save**

### Ejecutar el pipeline

1. En la lista de DAGs, buscar **`hostel_pipeline`**
2. Activar el toggle (si está pausado)
3. Click en ▶ **Trigger DAG**
4. La tarea `cargar_staging` tarda ~15 minutos por el volumen de datos — es normal

### Verificar que los datos cargaron

```bash
docker exec -it postgres-dwh psql -U dwh -d dwh -c "
SELECT 'stg_hostels' as tabla, COUNT(*) FROM stg_hostels
UNION ALL SELECT 'stg_reviews', COUNT(*) FROM stg_reviews
UNION ALL SELECT 'kpi_hostel_ratings', COUNT(*) FROM kpi_hostel_ratings
UNION ALL SELECT 'kpi_hostel_ranking', COUNT(*) FROM kpi_hostel_ranking;
"
```

Deberías ver ~41.642 hostels y ~934.000 reseñas.

---

## Configuración de Metabase

### Abrir la UI

Navegador → **http://localhost:3000**

La primera vez te pide crear una cuenta (nombre, email, contraseña — usá lo que quieras, es local).

### Conectar el DWH

Cuando pregunte "Add your data" → elegir **PostgreSQL**:

| Campo         | Valor          |
|---------------|----------------|
| Display name  | `DWH Hostels`  |
| Host          | `postgres-dwh` |
| Port          | `5432`         |
| Database name | `dwh`          |
| Username      | `dwh`          |
| Password      | `dwh123`       |

Click en **Save** y esperar que sincronice.

### Cargar las visualizaciones

Para cada query del archivo `kpi_marketing.sql` (y las de `guia_dashboard_marketing.md`):

1. **+ Nuevo → Pregunta → Consulta nativa**
2. Seleccionar base `DWH Hostels`
3. Pegar la query (una por vez)
4. Click en ▶ para ejecutar
5. Elegir tipo de visualización según el comentario de la query
6. **Guardar** con el nombre indicado

### Armar el dashboard

1. **+ Nuevo → Dashboard** → nombre: `Hostel Analytics`
2. Agregar todas las preguntas guardadas
3. Organizar por bloque arrastrando

### Compartir el dashboard con un compañero

En la terminal (dentro de la carpeta del proyecto):

```bash
# Exportar preguntas, dashboards y conexión
docker exec postgres-metabase pg_dump -U metabase --data-only \
  -t public.report_card -t public.report_dashboard -t public.report_dashboardcard \
  -t public.metabase_database -t public.collection \
  > export_metabase.sql
```

Tu compañero importa el archivo en su máquina:

```bash
cat export_metabase.sql | docker exec -i postgres-metabase psql -U metabase
```

> **Aviso:** La importación pisa el contenido existente de Metabase. Asegurate de que tu compañero no tenga cosas importantes antes de importar.

---

## Comandos útiles

```bash
# Ver estado de los contenedores
docker compose ps

# Ver logs del scheduler (útil si algo falla)
docker compose logs -f airflow-scheduler

# Apagar todo (los datos se conservan)
docker compose down

# Apagar y borrar todos los datos (reset total)
docker compose down -v

# Reconstruir la imagen de Airflow (si modifican el Dockerfile)
docker compose up -d --build
```

---

## Servicios y puertos

| Servicio   | URL                    | Credenciales                        |
|------------|------------------------|-------------------------------------|
| Airflow    | http://localhost:8080  | `admin` / `admin`                   |
| Metabase   | http://localhost:3000  | las que creaste en el primer acceso |
| PostgreSQL DWH | `localhost:5432`   | usuario `dwh`, contraseña `dwh123`  |

---

## Solución de problemas frecuentes

**Docker no encuentra el compose:**
```bash
# Asegurarse de estar en la carpeta del proyecto
cd ~/Documentos/proyecto_hostels
docker compose up -d --build
```

**La tarea cargar_staging falla con código -9:**
- Falta RAM. Ir a Docker Desktop → Settings → Resources → Memory → subir a 6 GB.

**No puedo conectarme a localhost después de reiniciar Docker:**
```bash
docker compose up -d
# esperar 30 segundos y volver a intentar
```

**Las tablas del DWH están vacías:**
- El DAG no corrió todavía o falló. Verificar en Airflow → `hostel_pipeline` → Grid.

**Metabase no muestra las tablas:**
- Ir a Admin → Databases → DWH Hostels → Sync database schema now.
