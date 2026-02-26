# Pipeline de Intensidad de Carbono - Medallion Architecture

Este repositorio parte del trabajo integrador del curso de Data Engineering (UTN), cuya entrega original es el notebook `Lucas_Diedrich_Trabajo_Final.ipynb`. A partir de ese punto de partida, el objetivo del repo es **modularizar el código en un paquete Python** y **extender la funcionalidad**, incorporando por ejemplo una capa Gold con métricas de negocio.

El pipeline consume la API pública de intensidad de carbono del Reino Unido (National Grid ESO) y almacena los datos en un data lake con arquitectura Medallion usando Delta Lake, aplicando patrones de producción: extracción incremental con seguimiento de estado, transformaciones con validación de calidad, y almacenamiento con soporte para ACID transactions y schema enforcement.

---

## Dashboard interactivo

El dashboard se publica automáticamente en GitHub Pages en cada push a `main`:
https://lucasarg00.github.io/UTN_CURSO_DE/

Incluye 6 visualizaciones Plotly construidas sobre las tablas Gold:
tendencia semanal de energía verde, calendario de precisión del forecast,
intensidad por periodo del día, distribución de error por categoría,
scatter forecast vs actual, y composición del grid en el tiempo.

---

## Arquitectura

El pipeline sigue el patrón Medallion con tres capas:

```
API (carbonintensity.org.uk)
        |
        v
   [Bronze Layer]  -- Datos crudos, sin modificar, histórico completo
        |
        v
   [Silver Layer]  -- Datos limpios, validados y enriquecidos
        |
        v
   [Gold Layer]    -- Tablas analíticas agregadas (diario, periodo, semanal)
```

Tanto la extracción como la transformación son **incrementales y stateful**: cada ejecución procesa solo los datos nuevos desde la última vez que corrió el pipeline.

---

## Estructura del repositorio

```
.
├── scripts/
│   ├── run_extraction.py       # Extrae datos de la API y los guarda en Bronze
│   ├── run_transformation.py   # Transforma datos de Bronze a Silver
│   └── run_gold.py             # Construye las tablas analíticas Gold desde Silver
│
├── src/
│   └── carbon_intensity/
│       ├── api_client.py       # Llamadas a la API (factores estáticos y datos temporales)
│       ├── config.py           # Rutas, endpoints y constantes de negocio
│       ├── gold_aggregations.py # Funciones de agregación pandas para Gold
│       ├── state_manager.py    # Lectura y escritura del estado del pipeline
│       ├── storage_delta.py    # Operaciones sobre tablas Delta Lake
│       └── transformations.py  # Limpieza, enriquecimiento y validación de datos
│
├── notebooks/
│   └── carbon_intensity_dashboard.qmd  # Dashboard Quarto con 6 visualizaciones Plotly
│
├── .github/workflows/
│   └── deploy_dashboard.yml    # CI/CD: extracción -> transformación -> Gold -> Pages
│
├── recomendaciones/
│   └── Medallion Architecture.txt  # Descripción de la arquitectura implementada
│
├── Lucas_Diedrich_Trabajo_Final.ipynb  # Entrega original del curso (punto de partida del repo)
└── requirements.txt                    # Dependencias del proyecto
```

> El directorio `datalake/` y el archivo de estado `metadata_carbon_intensity_final_report.json` se generan en tiempo de ejecución y no están versionados.

---

## Descripcion de los modulos

### `src/carbon_intensity/config.py`
Centraliza toda la configuración: URLs de la API, rutas del data lake, nombres de columnas y constantes de negocio (límite de 14 días de la API, umbrales de intensidad). Modificar aquí afecta a todo el pipeline sin tocar los demás módulos.

### `src/carbon_intensity/api_client.py`
Contiene las funciones que consumen la API de `carbonintensity.org.uk`:
- `get_carbon_intensity_factors()`: obtiene los factores de emisión por tipo de combustible (dato estático).
- `get_carbon_intensity_temporal(from, to)`: obtiene datos de intensidad de carbono cada 30 minutos para un rango de fechas.

### `src/carbon_intensity/state_manager.py`
Gestiona el archivo de estado `metadata_carbon_intensity_final_report.json`. Permite saber cuál fue el último registro extraído y transformado, para que cada ejecución del pipeline solo procese datos nuevos.

### `src/carbon_intensity/storage_delta.py`
Abstrae las operaciones de Delta Lake: crear o sobreescribir tablas, hacer MERGE para inserciones incrementales sin duplicados, leer tablas, agregar constraints de calidad, obtener estadísticas, y ejecutar COMPACT y VACUUM.

### `src/carbon_intensity/gold_aggregations.py`
Funciones puras pandas para construir las tres tablas Gold desde Silver:
- `build_daily_metrics()`: una fila por día con MAE, MAPE, percentiles y dominant_index.
- `build_period_efficiency()`: una fila por (periodo del día × weekday/weekend) con volatilidad y % alta intensidad.
- `build_sustainability_reports()`: una fila por semana ISO con distribución de índices y green_hours_pct.

### `src/carbon_intensity/transformations.py`
Implementa las transformaciones Bronze -> Silver:
- Renombrado de columnas
- Conversión y validación de tipos datetime
- Eliminación de duplicados y nulos
- Feature engineering: `is_weekend`, `period_of_day`
- Columna de partición: `fecha`

---

## Requisitos

- Python 3.13+

Instalar dependencias:

```bash
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

Las dependencias principales son: `requests`, `pandas`, `pyarrow`, `deltalake`, `plotly`.

---

## Inicializacion del archivo de estado

El pipeline requiere un archivo de estado para saber desde qué fecha extraer datos. Este archivo **no está incluido en el repositorio** y debe crearse manualmente antes de la primera ejecución.

Crear el archivo `metadata_carbon_intensity_final_report.json` en la raíz del proyecto con el siguiente contenido, ajustando la fecha de inicio deseada:

```json
{
    "half_hour_carbon_intensity": {
        "extraction": {
            "incremental_column": "from",
            "last_value": "2025-01-01T00:00Z"
        },
        "transformation": {
            "incremental_column": "from_time",
            "last_processed_timestamp": "2025-01-01T00:00Z"
        }
    }
}
```

> La API tiene un límite de **14 días hacia atrás**. Si la fecha configurada es anterior a ese límite, el pipeline lo detecta, emite una advertencia y ajusta la extracción a los últimos 14 días disponibles.

---

## Como correr el pipeline

Los scripts se ejecutan desde la raíz del repositorio.

### 1. Extraccion (API -> Bronze)

```bash
python scripts/run_extraction.py
```

Extrae dos tipos de datos:
- **Factores de combustible** (dato estático): se sobreescribe completamente en cada ejecución.
- **Intensidad de carbono** (dato temporal): extracción incremental desde la última fecha registrada en el estado. Usa MERGE para evitar duplicados.

### 2. Transformacion (Bronze -> Silver)

```bash
python scripts/run_transformation.py
```

Lee solo los datos no procesados de Bronze (desde el último timestamp transformado), aplica limpieza y enriquecimiento, y los guarda en Silver particionado por fecha. Al finalizar, verifica la calidad de los datos en Silver.

> Debe ejecutarse después de `run_extraction.py`. El script valida que Bronze exista y termina con error si no es así.

### 3. Gold (Silver -> tablas analíticas)

```bash
python scripts/run_gold.py
```

Construye las tres tablas Gold en `datalake/gold/` con full refresh (overwrite).
No requiere estado: agrega todos los datos de Silver en cada ejecución.

### Orden de ejecución

```
run_extraction.py  -->  run_transformation.py  -->  run_gold.py
```

Los tres scripts son idempotentes: se pueden re-ejecutar sin duplicar datos.

---

## Dashboard

El dashboard se genera a partir de las tablas Gold con Quarto y Plotly.

### Renderizar localmente

Con Quarto instalado:

```bash
quarto render notebooks/carbon_intensity_dashboard.qmd
# Genera: notebooks/carbon_intensity_dashboard.html
```

### Deploy automático

GitHub Actions ejecuta el pipeline completo (extracción -> transformación -> Gold -> render)
en cada push a `main` y publica el resultado en GitHub Pages.

---

## Operaciones de mantenimiento Delta Lake

Las tareas de optimización **no forman parte del pipeline principal** y deben ejecutarse por separado:

```python
from src.carbon_intensity import storage_delta, config

# Compactar archivos pequeños (mejora rendimiento de lectura)
storage_delta.optimize_delta_table(config.SILVER_INTENSITY_PATH)

# Eliminar versiones antiguas de archivos (hacer dry run primero)
storage_delta.vacuum_delta_table(config.SILVER_INTENSITY_PATH, dry_run=True)
storage_delta.vacuum_delta_table(config.SILVER_INTENSITY_PATH, dry_run=False)
```

---

## Fuente de datos

**UK Carbon Intensity API** - National Grid ESO
`https://api.carbonintensity.org.uk`

- `GET /intensity/factors`: factores de emisión de CO2 por tipo de combustible (gCO2/kWh)
- `GET /intensity/{from}/{to}`: intensidad de carbono en intervalos de 30 minutos para un rango de fechas (máximo 14 días)

La API es pública y no requiere autenticación.
