"""
Script de Agregación - Capa Gold

Construye las tres tablas analíticas Gold a partir de los datos Silver.
Gold usa full refresh (overwrite) en cada ejecución: no requiere state management.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from src.carbon_intensity import storage_delta, config, gold_aggregations


def build_gold_tables():
    """Lee Silver completo y escribe las tres tablas Gold en overwrite."""
    print("=" * 60)
    print("CONSTRUCCION DE TABLAS GOLD")
    print("=" * 60)

    if not storage_delta.table_exists(config.SILVER_INTENSITY_PATH):
        print("ERROR: Tabla Silver no existe. Ejecute primero run_transformation.py")
        return False

    print("\n1. LECTURA DE SILVER")
    print("-" * 40)
    df = storage_delta.read_delta_table(config.SILVER_INTENSITY_PATH)

    if df.empty:
        print("ERROR: La tabla Silver está vacía")
        return False

    print(f"Registros leídos de Silver: {len(df)}")

    success_count = 0

    print("\n2. DAILY CARBON METRICS")
    print("-" * 40)
    try:
        daily = gold_aggregations.build_daily_metrics(df)
        storage_delta.save_data_as_delta(
            daily,
            config.GOLD_DAILY_METRICS_PATH,
            mode="overwrite",
            description="Métricas diarias de intensidad de carbono - Gold Layer"
        )
        success_count += 1
        print("SUCCESS: daily_carbon_metrics guardada")
    except Exception as e:
        print(f"ERROR en daily_carbon_metrics: {e}")

    print("\n3. PERIOD EFFICIENCY")
    print("-" * 40)
    try:
        period = gold_aggregations.build_period_efficiency(df)
        storage_delta.save_data_as_delta(
            period,
            config.GOLD_PERIOD_EFFICIENCY_PATH,
            mode="overwrite",
            description="Eficiencia por periodo del día - Gold Layer"
        )
        success_count += 1
        print("SUCCESS: period_efficiency guardada")
    except Exception as e:
        print(f"ERROR en period_efficiency: {e}")

    print("\n4. SUSTAINABILITY REPORTS")
    print("-" * 40)
    try:
        sustainability = gold_aggregations.build_sustainability_reports(df)
        storage_delta.save_data_as_delta(
            sustainability,
            config.GOLD_SUSTAINABILITY_REPORTS_PATH,
            mode="overwrite",
            description="Reportes semanales de sostenibilidad - Gold Layer"
        )
        success_count += 1
        print("SUCCESS: sustainability_reports guardada")
    except Exception as e:
        print(f"ERROR en sustainability_reports: {e}")

    return success_count == 3


def verify_gold_tables():
    """Verifica que las tres tablas Gold existen y tienen datos."""
    print("\n" + "=" * 60)
    print("VERIFICACION DE TABLAS GOLD")
    print("=" * 60)

    tables = {
        "daily_carbon_metrics": config.GOLD_DAILY_METRICS_PATH,
        "period_efficiency": config.GOLD_PERIOD_EFFICIENCY_PATH,
        "sustainability_reports": config.GOLD_SUSTAINABILITY_REPORTS_PATH,
    }

    all_ok = True
    for name, path in tables.items():
        if storage_delta.table_exists(path):
            stats = storage_delta.get_table_stats(path)
            print(f"  {name}: {stats['total_records']} filas, {len(stats['columns'])} columnas")
        else:
            print(f"  ERROR: {name} no existe")
            all_ok = False

    return all_ok


def main():
    print("INICIANDO PIPELINE GOLD")
    print("=" * 70)

    success_count = 0
    total_tasks = 2

    if build_gold_tables():
        success_count += 1

    if verify_gold_tables():
        success_count += 1

    print("\n" + "=" * 70)
    print("RESUMEN GOLD")
    print("=" * 70)
    print(f"Tareas completadas: {success_count}/{total_tasks}")

    if success_count == total_tasks:
        print("SUCCESS: PIPELINE GOLD COMPLETADO EXITOSAMENTE")
    else:
        print("ERROR: PIPELINE GOLD COMPLETADO CON ERRORES")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
