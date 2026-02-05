"""
Operaciones de Almacenamiento Delta Lake

Maneja las operaciones de tablas Delta Lake para el pipeline de intensidad de carbono.
Nota: Las tareas de optimización (COMPACT/VACUUM) están deliberadamente separadas
del pipeline principal según las mejores prácticas recomendadas.
"""

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from typing import Optional, List, Union
from pathlib import Path


def save_data_as_delta(
    df: pd.DataFrame,
    path: Union[str, Path],
    storage_options: Optional[dict] = None,
    mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None,
    description: Optional[str] = None
) -> None:
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.

    Args:
        df (pd.DataFrame): El dataframe a guardar.
        path (str|Path): La ruta donde se guardará el dataframe en formato Delta Lake.
        storage_options (dict): Opciones de almacenamiento (None para local).
        mode (str): El modo de guardado ("overwrite", "append", "error", "ignore").
        partition_cols (list): Columnas por las que se particionará el dataframe.
        description (str): Descripción de la tabla.
    """
    try:
        print(f"Guardando {len(df)} registros en {path} (modo: {mode})")

        write_deltalake(
            path,
            df,
            mode=mode,
            storage_options=storage_options,
            partition_by=partition_cols,
            description=description
        )

        print(f"SUCCESS: Datos guardados exitosamente en Delta Lake")

        # Verificar que se guardó correctamente
        dt = DeltaTable(path, storage_options=storage_options)
        total_records = len(dt.to_pandas())
        print(f"SUCCESS: Verificación: {total_records} registros totales en la tabla")

    except Exception as e:
        print(f"ERROR: Error al guardar datos en Delta Lake: {e}")
        raise


def save_new_data_as_delta(
    new_data: pd.DataFrame,
    data_path: Union[str, Path],
    predicate: str,
    storage_options: Optional[dict] = None,
    partition_cols: Optional[List[str]] = None
) -> None:
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    evitando duplicados al comparar con datos ya existentes.

    Args:
        new_data (pd.DataFrame): Los datos que se desean guardar.
        data_path (str|Path): La ruta donde se guardará el dataframe.
        predicate (str): La condición de predicado para la operación MERGE.
        storage_options (dict): Opciones de almacenamiento (None para local).
        partition_cols (list): Columnas de partición si es tabla nueva.
    """
    try:
        print(f"Ejecutando MERGE de {len(new_data)} registros nuevos...")

        dt = DeltaTable(data_path, storage_options=storage_options)
        new_data_pa = pa.Table.from_pandas(new_data)

        # Realizar MERGE - insertar registros que no existen
        (dt.merge(
            source=new_data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        )
        .when_not_matched_insert_all()
        .execute())

        print(f"SUCCESS: Operación MERGE completada exitosamente")

        # Verificar resultado
        total_records = len(dt.to_pandas())
        print(f"SUCCESS: Total registros en tabla después del MERGE: {total_records}")

    except TableNotFoundError:
        print("Tabla no existe, creando nueva tabla Delta Lake...")
        save_data_as_delta(
            new_data,
            data_path,
            storage_options=storage_options,
            mode="overwrite",
            partition_cols=partition_cols
        )

    except Exception as e:
        print(f"ERROR: Error en operación MERGE: {e}")
        raise


def read_delta_table(
    path: Union[str, Path],
    storage_options: Optional[dict] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[List] = None
) -> pd.DataFrame:
    """
    Lee una tabla Delta Lake y devuelve un DataFrame.

    Args:
        path (str|Path): Ruta de la tabla Delta
        storage_options (dict): Opciones de almacenamiento
        columns (list): Columnas específicas a leer
        filters (list): Filtros de partición para optimizar lectura

    Returns:
        pd.DataFrame: DataFrame con los datos de la tabla
    """
    try:
        print(f"Leyendo tabla Delta Lake desde {path}")

        dt = DeltaTable(path, storage_options=storage_options)

        if filters or columns:
            # Usar to_pandas con filtros/columnas específicas para optimizar
            df = dt.to_pandas(columns=columns, filters=filters)
        else:
            df = dt.to_pandas()

        print(f"SUCCESS: Leídos {len(df)} registros desde Delta Lake")
        return df

    except Exception as e:
        print(f"ERROR: Error al leer tabla Delta Lake: {e}")
        raise


def table_exists(path: Union[str, Path], storage_options: Optional[dict] = None) -> bool:
    """
    Verifica si existe una tabla Delta Lake en la ruta especificada.

    Args:
        path (str|Path): Ruta de la tabla
        storage_options (dict): Opciones de almacenamiento

    Returns:
        bool: True si la tabla existe, False en caso contrario
    """
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        return True
    except TableNotFoundError:
        return False
    except Exception:
        return False


def get_table_schema(path: Union[str, Path], storage_options: Optional[dict] = None) -> dict:
    """
    Obtiene el esquema de una tabla Delta Lake.

    Args:
        path (str|Path): Ruta de la tabla
        storage_options (dict): Opciones de almacenamiento

    Returns:
        dict: Información del esquema de la tabla
    """
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        schema = dt.schema()

        print(f"Esquema de tabla {path}:")
        for field in schema:
            print(f"  {field.name}: {field.type}")

        return {
            "fields": [(field.name, str(field.type)) for field in schema],
            "total_fields": len(schema)
        }

    except Exception as e:
        print(f"ERROR: Error al obtener esquema: {e}")
        raise


def add_table_constraint(
    path: Union[str, Path],
    constraint_name: str,
    constraint_expr: str,
    storage_options: Optional[dict] = None
) -> None:
    """
    Agrega una restricción (constraint) a una tabla Delta Lake.

    Args:
        path (str|Path): Ruta de la tabla
        constraint_name (str): Nombre de la restricción
        constraint_expr (str): Expresión SQL de la restricción
        storage_options (dict): Opciones de almacenamiento
    """
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        dt.alter.add_constraint({constraint_name: constraint_expr})
        print(f"SUCCESS: Constraint '{constraint_name}' agregado exitosamente")

    except Exception as e:
        # Si el constraint ya existe, solo mostrar info
        if "already exists" in str(e):
            print(f"INFO: Constraint '{constraint_name}' ya existe")
        else:
            print(f"ERROR: Error al agregar constraint: {e}")
            raise


def get_table_stats(path: Union[str, Path], storage_options: Optional[dict] = None) -> dict:
    """
    Obtiene estadísticas básicas de una tabla Delta Lake.

    Args:
        path (str|Path): Ruta de la tabla
        storage_options (dict): Opciones de almacenamiento

    Returns:
        dict: Estadísticas de la tabla (registros, archivos, particiones)
    """
    try:
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pandas()

        stats = {
            "total_records": len(df),
            "total_files": len(dt.file_uris()),
            "columns": list(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024
        }

        # Si hay particiones, calcular estadísticas
        if hasattr(dt, 'get_add_actions'):
            try:
                partitions = df.groupby(df.columns[0]).size() if len(df) > 0 else {}
                stats["partitions"] = len(partitions)
            except:
                stats["partitions"] = "unknown"

        print(f"Estadísticas de tabla {path}:")
        print(f"  Registros: {stats['total_records']}")
        print(f"  Archivos: {stats['total_files']}")
        print(f"  Columnas: {len(stats['columns'])}")
        print(f"  Uso memoria: {stats['memory_usage_mb']:.2f} MB")

        return stats

    except Exception as e:
        print(f"ERROR: Error al obtener estadísticas: {e}")
        raise