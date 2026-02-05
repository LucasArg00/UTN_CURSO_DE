"""
Pipeline de Datos de Intensidad de Carbono

Pipeline de ingeniería de datos production-ready implementando arquitectura Medallion
para procesar datos de intensidad de carbono de la Red Nacional del Reino Unido.

Módulos:
- config: Configuración y constantes
- api_client: Utilidades de extracción de datos de API
- state_manager: Gestión de estado para procesamiento incremental
- storage_delta: Operaciones Delta Lake
- transformations: Limpieza de datos e ingeniería de características
"""

__version__ = "1.0.0"
__author__ = "Lucas Diedrich"