"""
Script de Transformación - Capa Silver

Ejecuta el pipeline de transformación desde la capa Bronze hacia Silver,
aplicando limpieza, validación y enriquecimiento de datos.
"""

import sys
from pathlib import Path

# Agregar el directorio padre al path para imports
sys.path.append(str(Path(__file__).parent.parent))

from src.carbon_intensity import (
    transformations,
    storage_delta,
    state_manager,
    config
)


def transform_carbon_intensity_data():
    """Transforma datos de intensidad de carbono de Bronze a Silver"""
    print("="*60)
    print("TRANSFORMACIÓN INCREMENTAL BRONZE → SILVER")
    print("="*60)

    try:
        # Leer estado de transformación
        state = state_manager.get_pipeline_state(use_legacy=True)
        table_name = 'half_hour_carbon_intensity'

        if not state or table_name not in state:
            print(f"ERROR: No se encontró configuración para tabla {table_name}")
            return False

        last_processed = state_manager.get_last_processed_timestamp(state, table_name)

        if last_processed is None:
            print(f"Primera ejecución de transformación - procesando todos los datos de Bronze")
        else:
            print(f"Última transformación: {last_processed}")

        # PASO 1: Leer solo datos no procesados de Bronze
        print("\n1. LECTURA INCREMENTAL DE BRONZE")
        print("-" * 40)
        df_raw = transformations.read_unprocessed_data_from_bronze(
            config.BRONZE_INTENSITY_PATH,
            last_processed,
            storage_options=None
        )

        if df_raw.empty:
            print("INFO: No hay datos nuevos para procesar. Transformación completada.")
            return True

        print(f"SUCCESS: Datos leídos: {len(df_raw)} registros nuevos")

        # PASO 2: Aplicar transformaciones básicas
        print("\n2. TRANSFORMACIONES BÁSICAS")
        print("-" * 40)
        df_transformed = transformations.apply_column_transformations(df_raw)

        # PASO 3: Limpieza de datos
        print("\n3. LIMPIEZA DE DATOS")
        print("-" * 40)

        # Eliminar duplicados
        df_cleaned = transformations.remove_duplicates(df_transformed, key_column='from_time', keep='last')

        # Eliminar valores nulos en columnas críticas
        df_cleaned = transformations.remove_null_values(df_cleaned, config.CRITICAL_COLUMNS)

        # PASO 4: Enriquecimiento de datos
        print("\n4. ENRIQUECIMIENTO DE DATOS")
        print("-" * 40)

        # Agregar columnas de análisis temporal
        df_enriched = transformations.add_time_features(df_cleaned, timestamp_column='from_time')

        # Agregar columnas de partición
        df_enriched = transformations.add_partitioning_columns(df_enriched, timestamp_column='from_time')

        # Resetear índice para evitar columna __index_level_0__
        df_enriched = df_enriched.reset_index(drop=True)

        # PASO 5: Validación
        print("\n5. VALIDACIÓN DE RESULTADOS")
        print("-" * 40)
        expected_columns = ['from_time', 'to_time', 'forecast', 'actual', 'index', 'is_weekend', 'period_of_day', 'fecha']
        if not transformations.validate_transformation_results(df_enriched, expected_columns):
            print("ERROR: Validación de transformación fallida")
            return False

        # PASO 6: Guardar en Silver
        print("\n6. GUARDADO EN CAPA SILVER")
        print("-" * 40)
        print(f"Guardando {len(df_enriched)} registros transformados...")
        print(f"Particiones afectadas: {df_enriched['fecha'].nunique()} días")

        storage_delta.save_new_data_as_delta(
            df_enriched,
            config.SILVER_INTENSITY_PATH,
            predicate="target.from_time = source.from_time",
            storage_options=None,
            partition_cols=["fecha"]
        )

        # PASO 7: Agregar constraints avanzados para Silver
        print("\n7. APLICACIÓN DE CONSTRAINTS")
        print("-" * 40)

        # Constraints de calidad de datos
        constraints = [
            ("fechas_no_nulas_silver", "from_time IS NOT NULL AND to_time IS NOT NULL"),
            ("intensidad_no_negativa", "forecast >= 0 AND actual >= 0"),
            ("index_valido", "index IN ('very low', 'low', 'moderate', 'high', 'very high')"),
            ("particion_no_nula", "fecha IS NOT NULL")
        ]

        for constraint_name, constraint_expr in constraints:
            storage_delta.add_table_constraint(
                config.SILVER_INTENSITY_PATH,
                constraint_name,
                constraint_expr
            )

        # PASO 8: Actualizar estado
        print("\n8. ACTUALIZACIÓN DE ESTADO")
        print("-" * 40)
        max_timestamp = df_enriched['from_time'].max()
        state_manager.update_last_processed_timestamp(
            state,
            config.LEGACY_STATE_FILE,
            table_name,
            max_timestamp
        )

        print("\nSUCCESS: TRANSFORMACIÓN INCREMENTAL COMPLETADA EXITOSAMENTE!")
        print(f"  Registros transformados: {len(df_enriched)}")
        print(f"  Último timestamp procesado: {max_timestamp}")
        print(f"  Rango temporal: {df_enriched['from_time'].min()} → {df_enriched['from_time'].max()}")

        return True

    except Exception as e:
        print(f"ERROR en transformación: {e}")
        return False


def verify_silver_quality():
    """Verifica la calidad de los datos en la capa Silver"""
    print("\n" + "="*60)
    print("VERIFICACIÓN DE CALIDAD - CAPA SILVER")
    print("="*60)

    try:
        if not storage_delta.table_exists(config.SILVER_INTENSITY_PATH):
            print("WARNING: Tabla Silver no existe")
            return False

        # Obtener estadísticas
        stats = storage_delta.get_table_stats(config.SILVER_INTENSITY_PATH)
        print(f"INFO: Tabla Silver contiene {stats['total_records']} registros")
        print(f"INFO: Distribuidos en {stats['total_files']} archivos")

        # Leer muestra para validación
        df_silver = storage_delta.read_delta_table(config.SILVER_INTENSITY_PATH)

        # Verificaciones de calidad
        print("\nVerificaciones de calidad:")
        print(f"  CHECK: Registros totales: {len(df_silver)}")
        print(f"  CHECK: Sin nulos en from_time: {df_silver['from_time'].isnull().sum() == 0}")
        print(f"  CHECK: Sin nulos en actual: {df_silver['actual'].isnull().sum() == 0}")
        print(f"  CHECK: Valores forecast no negativos: {(df_silver['forecast'] >= 0).all()}")
        print(f"  CHECK: Fechas únicas para partición: {df_silver['fecha'].nunique()} particiones")

        # Distribución temporal
        print(f"\nDistribución temporal:")
        weekend_dist = df_silver['is_weekend'].value_counts()
        for is_weekend, count in weekend_dist.items():
            label = "Fin de semana" if is_weekend else "Entre semana"
            print(f"  {label}: {count} ({count/len(df_silver)*100:.1f}%)")

        print("SUCCESS: Verificación de calidad completada")
        return True

    except Exception as e:
        print(f"ERROR en verificación de calidad: {e}")
        return False


def main():
    """Función principal que ejecuta el pipeline completo de transformación"""
    print("INICIANDO PIPELINE DE TRANSFORMACIÓN - CAPA SILVER")
    print("="*70)

    # Verificar que existe la tabla Bronze
    if not storage_delta.table_exists(config.BRONZE_INTENSITY_PATH):
        print("ERROR: Tabla Bronze no existe. Ejecute primero run_extraction.py")
        return 1

    success_count = 0
    total_tasks = 2

    # Ejecutar transformación
    if transform_carbon_intensity_data():
        success_count += 1

    # Verificar calidad
    if verify_silver_quality():
        success_count += 1

    print("\n" + "="*70)
    print("RESUMEN DE TRANSFORMACIÓN")
    print("="*70)
    print(f"Tareas completadas exitosamente: {success_count}/{total_tasks}")

    if success_count == total_tasks:
        print("SUCCESS: PIPELINE DE TRANSFORMACIÓN COMPLETADO EXITOSAMENTE")
    else:
        print("ERROR: PIPELINE DE TRANSFORMACIÓN COMPLETADO CON ERRORES")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)