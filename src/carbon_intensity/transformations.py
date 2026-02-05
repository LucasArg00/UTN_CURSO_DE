"""
Transformaciones de Datos para Pipeline de Intensidad de Carbono

Maneja la limpieza, validación y enriquecimiento de datos para la transformación
de la capa Bronze a Silver en la arquitectura Medallion.
"""

import pandas as pd
from datetime import datetime
from typing import List, Optional
from deltalake import DeltaTable
from .config import CRITICAL_COLUMNS, COLUMN_RENAMES


def remove_duplicates(df: pd.DataFrame, key_column: str = 'from_time', keep: str = 'last') -> pd.DataFrame:
    """
    Elimina registros duplicados quedándose con el más reciente

    Parámetros:
        df (pd.DataFrame): DataFrame a limpiar
        key_column (str): Columna clave para identificar duplicados
        keep (str): 'last' para quedarse con el más reciente, 'first' para el primero

    Retorna:
        pd.DataFrame: DataFrame sin duplicados
    """
    total_inicial = len(df)
    duplicados = df.duplicated(subset=[key_column], keep=False).sum()

    df_clean = df.drop_duplicates(subset=[key_column], keep=keep)

    total_final = len(df_clean)
    eliminados = total_inicial - total_final

    print(f"Detección de duplicados:")
    print(f"    Total registros iniciales: {total_inicial}")
    print(f"    Duplicados encontrados: {duplicados}")
    print(f"    Registros eliminados: {eliminados}")
    print(f"    Registros finales: {total_final}")

    return df_clean


def remove_null_values(df: pd.DataFrame, critical_columns: List[str]) -> pd.DataFrame:
    """
    Elimina registros con valores nulos en columnas críticas

    Parámetros:
        df (pd.DataFrame): DataFrame a limpiar
        critical_columns (list): Lista de columnas que no deben tener nulos

    Retorna:
        pd.DataFrame: DataFrame sin nulos en columnas críticas
    """
    total_inicial = len(df)

    print(f"Detección de valores nulos:")
    print(f"    Total registros iniciales: {total_inicial}")

    # Contar nulos por columna
    hay_nulos = False
    for col in critical_columns:
        if col in df.columns:
            nulos = df[col].isnull().sum()
            if nulos > 0:
                hay_nulos = True
                print(f"    {col}: {nulos} nulos ({nulos/total_inicial*100:.1f}%)")

    if not hay_nulos:
        print(f"    No se encontraron valores nulos en columnas críticas")

    # Identificar filas con nulos en columnas críticas antes de eliminar
    available_critical_columns = [col for col in critical_columns if col in df.columns]
    filas_con_nulos = df[df[available_critical_columns].isnull().any(axis=1)]

    if len(filas_con_nulos) > 0:
        print(f"    Se eliminarán {len(filas_con_nulos)} filas con nulos en columnas críticas")
        if len(filas_con_nulos) > 5:
            print(f"    Guardando filas eliminadas en 'eliminated_for_nulls.csv'")
            filas_con_nulos.to_csv("eliminated_for_nulls.csv", index=False)

    # Eliminar registros con nulos
    df_clean = df.dropna(subset=available_critical_columns)

    total_final = len(df_clean)
    eliminados = total_inicial - total_final

    print(f"    Registros eliminados: {eliminados}")
    print(f"    Registros finales: {total_final}")

    return df_clean


def add_time_features(df: pd.DataFrame, timestamp_column: str = 'from_time') -> pd.DataFrame:
    """
    Agrega columnas calculadas basadas en timestamp

    Parámetros:
        df (pd.DataFrame): DataFrame con columna de timestamp
        timestamp_column (str): Nombre de la columna datetime

    Retorna:
        pd.DataFrame: DataFrame con nuevas columnas: 'is_weekend', 'period_of_day'
    """
    # Asegurar que la columna es datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])

    # Crear is_weekend: True si sábado (5) o domingo (6)
    df['is_weekend'] = df[timestamp_column].dt.dayofweek >= 5

    # Crear period_of_day usando función auxiliar
    def get_period(hour):
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 19:
            return 'afternoon'
        else:
            return 'night'

    df['period_of_day'] = df[timestamp_column].dt.hour.apply(get_period)

    print(f"Columnas de tiempo agregadas: is_weekend, period_of_day")
    print(f"    Distribución weekend:")
    weekend_dist = df['is_weekend'].value_counts()
    for val, count in weekend_dist.items():
        label = "Fin de semana" if val else "Entre semana"
        print(f"    {label}: {count} ({count/len(df)*100:.1f}%)")

    print(f"   Distribución periodo del día:")
    period_dist = df['period_of_day'].value_counts()
    period_labels = {'morning': 'Mañana', 'afternoon': 'Tarde', 'night': 'Noche'}
    for period, count in period_dist.items():
        print(f"    {period_labels.get(period, period)}: {count} ({count/len(df)*100:.1f}%)")

    return df


def read_unprocessed_data_from_bronze(
    bronze_path: str,
    last_processed_timestamp: Optional[str],
    storage_options: Optional[dict] = None
) -> pd.DataFrame:
    """
    Lee de Bronze solo los registros que no han sido procesados a Silver

    Usa filtros de Delta Lake para lectura eficiente:
    1. Primero filtra por timestamp (lectura optimizada)
    2. Ordena por timestamp para procesamiento secuencial

    Parámetros:
        bronze_path (str): Ruta de la tabla Bronze
        last_processed_timestamp (str or None): Último timestamp procesado (ISO8601) o None
        storage_options (dict): Opciones de almacenamiento (None para local)

    Retorna:
        pd.DataFrame: DataFrame con solo los registros no procesados
    """
    print(f"Lectura incremental de Bronze")

    try:
        dt_bronze = DeltaTable(bronze_path, storage_options=storage_options)

        # Primera ejecución (procesar todo)
        if last_processed_timestamp is None:
            print(f"    Modo: PRIMERA EJECUCIÓN (procesar todos los datos de Bronze)")
            df_all = dt_bronze.to_pandas()
            print(f"    Registros encontrados: {len(df_all)}")
            return df_all

        # Ejecución incremental
        print(f"    Último procesado: {last_processed_timestamp}")

        # Leer todos los datos (para tablas pequeñas es eficiente)
        df_all = dt_bronze.to_pandas()

        # Convertir 'from' a datetime si no lo es
        timestamp_col = 'from' if 'from' in df_all.columns else 'from_time'
        if not pd.api.types.is_datetime64_any_dtype(df_all[timestamp_col]):
            df_all[timestamp_col] = pd.to_datetime(df_all[timestamp_col])

        # Filtrar por timestamp
        df_new = df_all[df_all[timestamp_col] > last_processed_timestamp].copy()

        # Ordenar por timestamp
        df_new = df_new.sort_values(timestamp_col)

        if len(df_new) > 0:
            print(f"    Registros nuevos encontrados: {len(df_new)}")
            print(f"    Rango: {df_new[timestamp_col].min()} → {df_new[timestamp_col].max()}")
        else:
            print(f"    No hay datos nuevos para procesar")

        return df_new

    except Exception as e:
        print(f"    ERROR: Error al leer Bronze: {e}")
        raise


def apply_column_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones básicas de columnas (renombrado, conversión de tipos)

    Parámetros:
        df (pd.DataFrame): DataFrame con datos crudos de Bronze

    Retorna:
        pd.DataFrame: DataFrame con columnas transformadas
    """
    print(f"Aplicando transformaciones básicas de columnas...")

    # Crear copia para evitar modificar original
    df_transformed = df.copy()

    # Renombrar columnas usando mapping de configuración
    df_transformed.columns = df_transformed.columns.str.replace('intensity.', '', regex=False)
    df_transformed = df_transformed.rename(columns=COLUMN_RENAMES)

    # Convertir fechas a datetime si no lo son
    timestamp_columns = ['from_time', 'to_time']
    for col in timestamp_columns:
        if col in df_transformed.columns:
            if not pd.api.types.is_datetime64_any_dtype(df_transformed[col]):
                df_transformed[col] = pd.to_datetime(df_transformed[col])
                print(f"    SUCCESS: Convertido {col} a datetime")

    print(f"    SUCCESS: Transformaciones de columnas completadas")
    print(f"    Columnas finales: {list(df_transformed.columns)}")

    return df_transformed


def add_partitioning_columns(df: pd.DataFrame, timestamp_column: str = 'from_time') -> pd.DataFrame:
    """
    Agrega columnas necesarias para particionado de Delta Lake

    Parámetros:
        df (pd.DataFrame): DataFrame con timestamp
        timestamp_column (str): Columna de timestamp para extraer fecha

    Retorna:
        pd.DataFrame: DataFrame con columna 'fecha' para particionado
    """
    if timestamp_column in df.columns:
        # Crear columna de fecha para particionado
        df['fecha'] = df[timestamp_column].dt.date
        print(f"    SUCCESS: Agregada columna 'fecha' para particionado")

        # Estadísticas de particiones
        unique_dates = df['fecha'].nunique()
        print(f"    Particiones que se crearán: {unique_dates} días")

    return df


def validate_transformation_results(
    df: pd.DataFrame,
    expected_columns: List[str],
    min_records: int = 1
) -> bool:
    """
    Valida que los resultados de transformación sean correctos

    Parámetros:
        df (pd.DataFrame): DataFrame transformado
        expected_columns (list): Columnas que deben estar presentes
        min_records (int): Mínimo número de registros esperados

    Retorna:
        bool: True si la validación pasa, False en caso contrario
    """
    print(f"Validando resultados de transformación...")

    # Validar que no esté vacío
    if len(df) < min_records:
        print(f"    ERROR: DataFrame tiene solo {len(df)} registros, se esperaban al menos {min_records}")
        return False

    # Validar columnas requeridas
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"    ERROR: Faltan columnas requeridas: {missing_columns}")
        return False

    # Validar que las fechas estén en el rango correcto
    if 'from_time' in df.columns:
        min_date = df['from_time'].min()
        max_date = df['from_time'].max()
        print(f"    INFO: Rango temporal: {min_date} → {max_date}")

    print(f"    SUCCESS: Validación completada exitosamente")
    print(f"    Registros válidos: {len(df)}")
    print(f"    Columnas: {len(df.columns)}")

    return True