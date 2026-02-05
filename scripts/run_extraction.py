"""
Script de Extracción - Capa Bronze

Ejecuta el pipeline de extracción de datos desde la API de intensidad de carbono
del Reino Unido hacia la capa Bronze (datos crudos) del data lake.
"""

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Agregar el directorio padre al path para imports
sys.path.append(str(Path(__file__).parent.parent))

from src.carbon_intensity import (
    api_client,
    storage_delta,
    state_manager,
    config
)


def extract_static_data():
    """Extrae datos estáticos (factores de combustible) con refresh completo"""
    print("="*50)
    print("EXTRACCIÓN DE DATOS ESTÁTICOS - FACTORES DE COMBUSTIBLE")
    print("="*50)

    try:
        # Obtener factores de combustible
        df_factors = api_client.get_carbon_intensity_factors()

        if df_factors is None or df_factors.empty:
            print("ERROR: No se pudieron obtener factores de combustible")
            return False

        # Guardar con modo overwrite (refresh completo)
        storage_delta.save_data_as_delta(
            df_factors,
            config.BRONZE_FACTORS_PATH,
            mode="overwrite",
            description="Factores de Emisión de Combustible - Datos Estáticos"
        )

        # Agregar constraints básicos
        storage_delta.add_table_constraint(
            config.BRONZE_FACTORS_PATH,
            "factores_no_negativos",
            "Biomass >= 0 AND Coal >= 0 AND `Gas (Combined Cycle)` >= 0"
        )

        print("SUCCESS: Extracción de datos estáticos completada exitosamente")
        return True

    except Exception as e:
        print(f"ERROR en extracción de datos estáticos: {e}")
        return False


def extract_temporal_data():
    """Extrae datos temporales con procesamiento incremental stateful"""
    print("="*50)
    print("EXTRACCIÓN INCREMENTAL DE DATOS TEMPORALES")
    print("="*50)

    try:
        # Leer estado actual
        state = state_manager.get_pipeline_state(use_legacy=True)
        table_name = 'half_hour_carbon_intensity'

        if not state or table_name not in state:
            print(f"ERROR: No se encontró configuración para tabla {table_name}")
            return False

        # Obtener último valor procesado
        last_value_str = state_manager.get_last_incremental_value(state, table_name)
        last_value = datetime.fromisoformat(last_value_str.replace('Z', '+00:00'))

        # Calcular fecha actual (UTC)
        fecha_actual = datetime.now(timezone.utc)

        # Validar limitación de la API (14 días máximo)
        dias_diferencia = (fecha_actual - last_value).days

        print(f"Estado de la extracción:")
        print(f"    Última extracción: {last_value_str}")
        print(f"    Fecha actual: {fecha_actual.strftime('%Y-%m-%dT%H:%MZ')}")
        print(f"    Diferencia: {dias_diferencia} días")

        if dias_diferencia > config.API_MAX_DAYS_BACK:
            print(f"\nADVERTENCIA: Diferencia de {dias_diferencia} días detectada")
            print(f"Se extraerán datos desde hace {config.API_MAX_DAYS_BACK} días hasta ahora")

            fecha_gap_inicio = fecha_actual - timedelta(days=config.API_MAX_DAYS_BACK)
            print(f"Se perderán datos entre {last_value_str} y {fecha_gap_inicio.strftime('%Y-%m-%dT%H:%MZ')}")

            # Ajustar fecha de inicio
            fecha_inicio = fecha_actual - timedelta(days=config.API_MAX_DAYS_BACK)
        else:
            fecha_inicio = last_value
            print(f"\nExtracción incremental normal desde última fecha registrada")

        # Obtener datos temporales
        df_intensity = api_client.get_carbon_intensity_temporal(fecha_inicio, fecha_actual)

        if df_intensity is None or df_intensity.empty:
            print("INFO: No hay datos nuevos para procesar")
            return True

        # Guardar con MERGE para evitar duplicados
        print(f"\nGuardando en capa Bronze...")
        storage_delta.save_new_data_as_delta(
            df_intensity,
            config.BRONZE_INTENSITY_PATH,
            predicate="target.`from` = source.`from`",
            storage_options=None
        )

        # Agregar constraints básicos
        storage_delta.add_table_constraint(
            config.BRONZE_INTENSITY_PATH,
            "fechas_no_nulas",
            "`from` IS NOT NULL AND `to` IS NOT NULL"
        )

        # Actualizar estado
        max_from_time = df_intensity['from'].max()
        state_manager.update_incremental_value(
            state,
            config.LEGACY_STATE_FILE,
            table_name,
            max_from_time
        )

        print("SUCCESS: Extracción incremental completada exitosamente")
        return True

    except Exception as e:
        print(f"ERROR en extracción temporal: {e}")
        return False


def main():
    """Función principal que ejecuta el pipeline completo de extracción"""
    print("INICIANDO PIPELINE DE EXTRACCIÓN - CAPA BRONZE")
    print("="*60)

    # Contador de éxitos
    success_count = 0
    total_tasks = 2

    # Ejecutar extracción de datos estáticos
    if extract_static_data():
        success_count += 1

    print()  # Línea en blanco para separación

    # Ejecutar extracción de datos temporales
    if extract_temporal_data():
        success_count += 1

    print()
    print("="*60)
    print("RESUMEN DE EXTRACCIÓN")
    print("="*60)
    print(f"Tareas completadas exitosamente: {success_count}/{total_tasks}")

    if success_count == total_tasks:
        print("SUCCESS: PIPELINE DE EXTRACCIÓN COMPLETADO EXITOSAMENTE")

        # Mostrar estadísticas finales
        print("\nEstadísticas finales:")
        try:
            stats_factors = storage_delta.get_table_stats(config.BRONZE_FACTORS_PATH)
            stats_intensity = storage_delta.get_table_stats(config.BRONZE_INTENSITY_PATH)
            print(f"  Factores: {stats_factors['total_records']} registros en {stats_factors['total_files']} archivos")
            print(f"  Intensidad: {stats_intensity['total_records']} registros en {stats_intensity['total_files']} archivos")
        except Exception as e:
            print(f"  No se pudieron obtener estadísticas: {e}")
    else:
        print("ERROR: PIPELINE DE EXTRACCIÓN COMPLETADO CON ERRORES")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)