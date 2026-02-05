"""
Módulo de Configuración para Pipeline de Intensidad de Carbono

Centraliza todas las constantes de configuración, endpoints de API, rutas y constantes
de lógica de negocio para evitar hardcoding en toda la aplicación.
"""

import os
from pathlib import Path

# API Configuration
API_BASE_URL = "https://api.carbonintensity.org.uk"

ENDPOINTS = {
    "static": "intensity/factors",
    "temporal": "intensity/{from_time}/{to_time}"
}

# Storage Paths Configuration
DATA_ROOT = Path(os.getenv("DATA_ROOT", "datalake"))

# Bronze Layer Paths (Raw Data)
BRONZE_BASE = DATA_ROOT / "bronze" / "carbon_intensity_api"
BRONZE_FACTORS_PATH = BRONZE_BASE / "factors"
BRONZE_INTENSITY_PATH = BRONZE_BASE / "half_hour_carbon_intensity"

# Silver Layer Paths (Transformed Data)
SILVER_BASE = DATA_ROOT / "silver" / "carbon_intensity_api"
SILVER_INTENSITY_PATH = SILVER_BASE / "half_hour_carbon_intensity"

# Gold Layer Paths (Analytics)
GOLD_BASE = DATA_ROOT / "gold" / "carbon_intensity_api"
GOLD_DAILY_METRICS_PATH = GOLD_BASE / "daily_carbon_metrics"
GOLD_PERIOD_EFFICIENCY_PATH = GOLD_BASE / "period_efficiency"
GOLD_SUSTAINABILITY_REPORTS_PATH = GOLD_BASE / "sustainability_reports"

# State Management Configuration
STATE_FILE = "pipeline_state.json"
LEGACY_STATE_FILE = "metadata_carbon_intensity_final_report.json"

# Business Logic Constants
CARBON_INTENSITY_THRESHOLDS = {
    "very_low": 50,
    "low": 100,
    "moderate": 200,
    "high": 300,
    "very_high": 400
}

# Time Configuration
API_MAX_DAYS_BACK = 14  # UK Carbon Intensity API limitation
RETENTION_HOURS = 168   # 7 days for VACUUM operations

# Column Mapping for Transformations
COLUMN_RENAMES = {
    "intensity.forecast": "forecast",
    "intensity.actual": "actual",
    "intensity.index": "index",
    "from": "from_time",
    "to": "to_time"
}

# Critical columns that cannot have null values
CRITICAL_COLUMNS = ["from_time", "to_time", "forecast", "actual", "index"]

# Valid carbon intensity index values
VALID_INTENSITY_INDICES = ["very low", "low", "moderate", "high", "very high"]