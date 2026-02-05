"""
Cliente API para Datos de Intensidad de Carbono del Reino Unido

Maneja la comunicación con la API de intensidad de carbono del Reino Unido,
incluyendo extracción de datos y construcción de DataFrames con manejo de errores.
"""

import requests
import pandas as pd
from typing import Optional, Dict, List, Any
from .config import API_BASE_URL, ENDPOINTS, API_MAX_DAYS_BACK
from datetime import datetime, timedelta, timezone


def get_data(
    endpoint: str,
    data_field: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None
) -> Optional[List[Dict[str, Any]]]:
    """
    Realiza una solicitud GET a la UK Carbon Intensity API para obtener datos.

    Parámetros:
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    data_field (str): Atribudo del json de respuesta donde estará la lista
                     de objetos con los datos que requerimos
    params (dict): Parámetros de consulta para enviar con la solicitud.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    list: Los datos obtenidos de la API en formato JSON, o None si hay error.
    """
    try:
        endpoint_url = f"{API_BASE_URL}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error HTTP

        # Verificar si los datos están en formato JSON
        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except ValueError:
            print(f"ERROR: La respuesta no está en formato JSON válido")
            return None

        return data

    except requests.exceptions.RequestException as e:
        print(f"ERROR: La petición ha fallado. Código de error: {e}")
        return None
    except Exception as e:
        print(f"ERROR: Error inesperado al procesar la respuesta: {e}")
        return None


def build_table(
    json_data: List[Dict[str, Any]],
    record_path: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    json_data (list): Los datos en formato JSON obtenidos de una API.
    record_path (str): Ruta opcional para datos anidados.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos, o None si hay error.
    """
    try:
        if not json_data:
            print("WARNING: No hay datos para procesar")
            return pd.DataFrame()

        df = pd.json_normalize(json_data, record_path=record_path)
        return df

    except Exception as e:
        print(f"ERROR: Los datos no están en el formato esperado: {e}")
        return None


def get_carbon_intensity_factors() -> Optional[pd.DataFrame]:
    """
    Obtiene los factores de emisión de combustible (datos estáticos).

    Retorna:
    DataFrame: DataFrame con los factores de emisión por tipo de combustible.
    """
    print("Extrayendo factores de emisión de combustible...")

    factors_data = get_data(ENDPOINTS["static"], data_field="data")
    if factors_data is None:
        print("ERROR: No se pudieron obtener los factores de emisión")
        return None

    df_factors = build_table(factors_data)
    if df_factors is not None:
        print(f"SUCCESS: Factores de emisión obtenidos: {len(df_factors)} registros")

    return df_factors


def get_carbon_intensity_temporal(
    from_time: datetime,
    to_time: datetime
) -> Optional[pd.DataFrame]:
    """
    Obtiene datos temporales de intensidad de carbono.

    Parámetros:
    from_time (datetime): Fecha/hora de inicio
    to_time (datetime): Fecha/hora de fin

    Retorna:
    DataFrame: DataFrame con datos temporales de intensidad de carbono.
    """
    # Validar que el rango no exceda el límite de la API
    days_difference = (to_time - from_time).days
    if days_difference > API_MAX_DAYS_BACK:
        print(f"WARNING: El rango solicitado ({days_difference} días) excede el límite de la API ({API_MAX_DAYS_BACK} días)")
        # Ajustar fecha de inicio
        from_time = to_time - timedelta(days=API_MAX_DAYS_BACK)
        print(f"Ajustando fecha de inicio a: {from_time.strftime('%Y-%m-%dT%H:%MZ')}")

    # Construir endpoint dinámico
    from_param = from_time.strftime("%Y-%m-%dT%H:%MZ")
    to_param = to_time.strftime("%Y-%m-%dT%H:%MZ")
    endpoint = ENDPOINTS["temporal"].format(from_time=from_param, to_time=to_param)

    print(f"Extrayendo datos temporales desde {from_param} hasta {to_param}...")

    intensity_data = get_data(endpoint, data_field="data")
    if intensity_data is None:
        print("ERROR: No se pudieron obtener los datos temporales")
        return None

    df_intensity = build_table(intensity_data)
    if df_intensity is not None:
        print(f"SUCCESS: Datos temporales obtenidos: {len(df_intensity)} registros")

    return df_intensity


def validate_api_response(df: pd.DataFrame, expected_columns: List[str]) -> bool:
    """
    Valida que la respuesta de la API tenga las columnas esperadas.

    Parámetros:
    df (DataFrame): DataFrame a validar
    expected_columns (list): Lista de columnas que deben estar presentes

    Retorna:
    bool: True si todas las columnas están presentes, False en caso contrario
    """
    if df is None or df.empty:
        print("ERROR: DataFrame está vacío o es None")
        return False

    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"ERROR: Faltan columnas en la respuesta de la API: {missing_columns}")
        return False

    print("SUCCESS: Validación de respuesta API exitosa")
    return True