"""
Gestión de Estado para Procesamiento Incremental

Administra el estado del pipeline tanto para fases de extracción como transformación,
asegurando procesamiento incremental sin duplicación o reprocesamiento de datos.
"""

import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path
from .config import STATE_FILE, LEGACY_STATE_FILE


def read_state_from_json(file_path: str) -> Dict[str, Any]:
    """
    Lee un archivo JSON que contiene el último valor incremental extraído

    Parámetros:
        file_path (str): Ruta del archivo JSON

    Retorna:
        dict: Diccionario con el estado de la extracción
    """
    try:
        with open(file_path, 'r') as file:
            state = json.load(file)
            return state
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"El archivo JSON en la ruta {file_path} no es válido.")


def write_state_to_json(file_path: str, state: Dict[str, Any]) -> None:
    """
    Escribe el estado de la extracción en un archivo JSON

    Parámetros:
        file_path (str): Ruta del archivo JSON
        state (dict): Objeto con el estado de la extracción
    """
    try:
        with open(file_path, 'w') as file:
            json.dump(state, file, default=str, indent=4)
    except Exception as e:
        raise Exception(f"Error al escribir el archivo JSON: {e}")


def get_last_incremental_value(state: Dict[str, Any], table_name: str) -> str:
    """
    Obtiene el último valor incremental de extracción de una tabla

    Parámetros:
        state (dict): Objeto con el estado de la extracción
        table_name (str): Nombre de la tabla

    Retorna:
        str: Último valor incremental de la tabla (formato ISO8601)
    """
    try:
        return state[table_name]['extraction']['last_value']
    except KeyError:
        raise KeyError(f"La tabla {table_name} no existe en el archivo JSON o no tiene sección 'extraction'.")


def update_incremental_value(
    state: Dict[str, Any],
    file_path: str,
    table_name: str,
    new_value: str
) -> None:
    """
    Actualiza el valor incremental de extracción de una tabla

    Parámetros:
        state (dict): Objeto con el estado de la extracción
        file_path (str): Ruta donde guardar el archivo JSON
        table_name (str): Nombre de la tabla
        new_value (str): Nuevo valor incremental en formato ISO8601
    """
    last_value_str = get_last_incremental_value(state, table_name)

    # Convertir a datetime para comparar
    if isinstance(new_value, str):
        new_value_dt = datetime.fromisoformat(new_value.replace('Z', '+00:00'))
    else:
        new_value_dt = new_value

    last_value_dt = datetime.fromisoformat(last_value_str.replace('Z', '+00:00'))

    # Validaciones
    if new_value_dt <= last_value_dt:
        raise ValueError(f"El nuevo valor {new_value_dt} debe ser mayor que el anterior {last_value_dt}")
    if new_value is None:
        raise ValueError("El nuevo valor no puede ser nulo")

    # Actualizar el valor incremental
    new_value_formatted = new_value_dt.strftime("%Y-%m-%dT%H:%MZ")
    state[table_name]['extraction']['last_value'] = new_value_formatted
    write_state_to_json(file_path, state)
    print(f"SUCCESS: Estado actualizado: última extracción registrada en {new_value_formatted}")


def get_last_processed_timestamp(state: Dict[str, Any], table_name: str) -> Optional[str]:
    """
    Obtiene el último timestamp procesado a Silver

    Parámetros:
        state (dict): Objeto con el estado
        table_name (str): Nombre de la tabla

    Retorna:
        str or None: Último timestamp procesado (formato ISO8601) o None si es primera ejecución
    """
    try:
        last_processed = state[table_name]['transformation']['last_processed_timestamp']
        return last_processed
    except KeyError:
        raise KeyError(f"La tabla {table_name} no existe en el archivo JSON o no tiene sección 'transformation'.")


def update_last_processed_timestamp(
    state: Dict[str, Any],
    file_path: str,
    table_name: str,
    new_timestamp: str
) -> None:
    """
    Actualiza el último timestamp procesado a Silver

    Parámetros:
        state (dict): Objeto con el estado
        file_path (str): Ruta donde guardar el archivo JSON
        table_name (str): Nombre de la tabla
        new_timestamp (str): Nuevo timestamp procesado en formato ISO8601
    """
    last_processed = get_last_processed_timestamp(state, table_name)

    # Convertir a datetime para comparar
    if isinstance(new_timestamp, str):
        new_timestamp_dt = datetime.fromisoformat(new_timestamp.replace('Z', '+00:00'))
    else:
        new_timestamp_dt = new_timestamp

    # Validar solo si hay un valor anterior
    if last_processed is not None:
        last_processed_dt = datetime.fromisoformat(last_processed.replace('Z', '+00:00'))

        if new_timestamp_dt <= last_processed_dt:
            raise ValueError(f"El nuevo timestamp {new_timestamp_dt} debe ser mayor que el anterior {last_processed_dt}")

    # Actualizar timestamp procesado
    new_timestamp_formatted = new_timestamp_dt.strftime("%Y-%m-%dT%H:%MZ")
    state[table_name]['transformation']['last_processed_timestamp'] = new_timestamp_formatted
    write_state_to_json(file_path, state)
    print(f"SUCCESS: Estado de transformación actualizado: last_processed = {new_timestamp_formatted}")


def validate_state_file(file_path: str) -> bool:
    """
    Valida que el archivo de estado tenga la estructura correcta

    Parámetros:
        file_path (str): Ruta del archivo de estado

    Retorna:
        bool: True si el archivo es válido, False en caso contrario
    """
    try:
        state = read_state_from_json(file_path)

        # Verificar que tenga al menos una tabla configurada
        if not state:
            print("ERROR: Archivo de estado está vacío")
            return False

        # Verificar estructura mínima para cada tabla
        for table_name, table_config in state.items():
            if 'extraction' not in table_config:
                print(f"ERROR: Falta sección 'extraction' para tabla {table_name}")
                return False

            if 'transformation' not in table_config:
                print(f"ERROR: Falta sección 'transformation' para tabla {table_name}")
                return False

        print("SUCCESS: Archivo de estado válido")
        return True

    except Exception as e:
        print(f"ERROR: Error al validar archivo de estado: {e}")
        return False


def init_state_file(file_path: str, table_name: str, initial_timestamp: str) -> None:
    """
    Inicializa un archivo de estado para una nueva tabla

    Parámetros:
        file_path (str): Ruta donde crear el archivo
        table_name (str): Nombre de la tabla
        initial_timestamp (str): Timestamp inicial en formato ISO8601
    """
    initial_state = {
        table_name: {
            "extraction": {
                "incremental_column": "from",
                "last_value": initial_timestamp
            },
            "transformation": {
                "incremental_column": "from_time",
                "last_processed_timestamp": None
            }
        }
    }

    write_state_to_json(file_path, initial_state)
    print(f"SUCCESS: Archivo de estado inicializado para tabla {table_name}")


def get_pipeline_state(use_legacy: bool = True) -> Dict[str, Any]:
    """
    Obtiene el estado actual del pipeline, usando el archivo legacy por defecto

    Parámetros:
        use_legacy (bool): Si usar el archivo legacy o el nuevo formato

    Retorna:
        dict: Estado completo del pipeline
    """
    state_file = LEGACY_STATE_FILE if use_legacy else STATE_FILE

    if not Path(state_file).exists():
        print(f"WARNING: Archivo de estado {state_file} no existe")
        return {}

    return read_state_from_json(state_file)