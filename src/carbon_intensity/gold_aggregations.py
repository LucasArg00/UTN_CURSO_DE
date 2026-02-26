"""
Gold Layer Aggregations

Pure pandas aggregation functions that transform Silver data into analytical
Gold tables. All tables use full refresh (overwrite) on each run — no state
management needed because Gold is derived entirely from Silver.
"""

import pandas as pd


def build_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates Silver data to one row per day.

    Computes forecast/actual statistics, MAE, MAPE (excluding actual=0 rows),
    dominant intensity index, and record count.
    """
    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['from_time']):
        df['from_time'] = pd.to_datetime(df['from_time'])

    df['abs_error'] = (df['forecast'] - df['actual']).abs()

    # MAPE: only where actual > 0 to avoid division by zero
    df_valid = df[df['actual'] > 0].copy()
    df_valid['pct_error'] = df_valid['abs_error'] / df_valid['actual']

    daily = df.groupby('fecha').agg(
        avg_forecast=('forecast', 'mean'),
        avg_actual=('actual', 'mean'),
        min_actual=('actual', 'min'),
        max_actual=('actual', 'max'),
        p25_actual=('actual', lambda x: x.quantile(0.25)),
        p75_actual=('actual', lambda x: x.quantile(0.75)),
        mae=('abs_error', 'mean'),
        record_count=('actual', 'count'),
    ).reset_index()

    mape = (
        df_valid.groupby('fecha')['pct_error']
        .mean()
        .mul(100)
        .rename('mape')
        .reset_index()
    )

    dominant_index = (
        df.groupby('fecha')['index']
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        .rename('dominant_index')
        .reset_index()
    )

    # is_weekend is consistent within a day — take the first value
    is_weekend = df.groupby('fecha')['is_weekend'].first().reset_index()

    daily = (
        daily
        .merge(mape, on='fecha', how='left')
        .merge(dominant_index, on='fecha', how='left')
        .merge(is_weekend, on='fecha', how='left')
    )

    daily['min_actual'] = daily['min_actual'].astype('int64')
    daily['max_actual'] = daily['max_actual'].astype('int64')
    daily['record_count'] = daily['record_count'].astype('int64')

    print(f"daily_carbon_metrics: {len(daily)} rows")
    return daily


def build_period_efficiency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates Silver data to one row per (period_of_day, is_weekend) — 6 rows total.

    avg_volatility is the mean of daily std(actual) within each segment,
    which captures intra-day variability rather than segment-level spread.
    """
    df = df.copy()
    df['abs_error'] = (df['forecast'] - df['actual']).abs()
    df['is_high_intensity'] = df['index'].isin(['high', 'very high'])

    df_valid = df[df['actual'] > 0].copy()
    df_valid['pct_error'] = df_valid['abs_error'] / df_valid['actual']

    key = ['period_of_day', 'is_weekend']

    base = df.groupby(key).agg(
        avg_actual=('actual', 'mean'),
        avg_forecast=('forecast', 'mean'),
        mae=('abs_error', 'mean'),
        pct_high_intensity=('is_high_intensity', 'mean'),
        record_count=('actual', 'count'),
    ).reset_index()

    mape = (
        df_valid.groupby(key)['pct_error']
        .mean()
        .mul(100)
        .rename('mape')
        .reset_index()
    )

    # std(actual) per day per segment, then average those stds
    daily_vol = (
        df.groupby(['fecha'] + key)['actual']
        .std()
        .reset_index()
        .groupby(key)['actual']
        .mean()
        .rename('avg_volatility')
        .reset_index()
    )

    result = (
        base
        .merge(mape, on=key, how='left')
        .merge(daily_vol, on=key, how='left')
    )

    result['pct_high_intensity'] = result['pct_high_intensity'] * 100
    result['record_count'] = result['record_count'].astype('int64')

    print(f"period_efficiency: {len(result)} rows")
    return result


def build_sustainability_reports(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates Silver data to one row per ISO calendar week.

    Uses isocalendar().year (not .dt.year) to correctly handle December days
    that belong to week 1 of the following year.
    """
    df = df.copy()

    if not pd.api.types.is_datetime64_any_dtype(df['from_time']):
        df['from_time'] = pd.to_datetime(df['from_time'])

    iso = df['from_time'].dt.isocalendar()
    df['iso_year'] = iso['year'].astype(int)
    df['iso_week'] = iso['week'].astype(int)
    df['year_week'] = df['iso_year'].astype(str) + '-W' + df['iso_week'].astype(str).str.zfill(2)

    # Monday of each ISO week
    df['week_start'] = (
        df['from_time'] - pd.to_timedelta(df['from_time'].dt.dayofweek, unit='D')
    ).dt.normalize().dt.date

    df['abs_error'] = (df['forecast'] - df['actual']).abs()

    df_valid = df[df['actual'] > 0].copy()
    df_valid['pct_error'] = df_valid['abs_error'] / df_valid['actual']

    key = ['year_week', 'iso_year', 'iso_week']

    base = df.groupby(key).agg(
        avg_actual=('actual', 'mean'),
        avg_forecast=('forecast', 'mean'),
        mae=('abs_error', 'mean'),
        record_count=('actual', 'count'),
    ).reset_index()

    mape = (
        df_valid.groupby(key)['pct_error']
        .mean()
        .mul(100)
        .rename('mape')
        .reset_index()
    )

    week_start = (
        df.groupby('year_week')['week_start']
        .first()
        .rename('week_start_date')
        .reset_index()
    )

    # Index distribution as percentage of weekly records
    total_per_week = df.groupby('year_week')['actual'].count().rename('_total')
    index_counts = df.groupby(['year_week', 'index'])['actual'].count().unstack(fill_value=0)

    for idx in ['very low', 'low', 'moderate', 'high', 'very high']:
        if idx not in index_counts.columns:
            index_counts[idx] = 0

    index_pct = (
        index_counts
        .div(total_per_week, axis=0)
        .mul(100)
        .reset_index()
        .rename(columns={
            'very low': 'pct_very_low',
            'low': 'pct_low',
            'moderate': 'pct_moderate',
            'high': 'pct_high',
            'very high': 'pct_very_high',
        })
    )

    result = (
        base
        .rename(columns={'iso_year': 'year', 'iso_week': 'week_number'})
        .merge(
            mape.drop(columns=['iso_year', 'iso_week']),
            on='year_week', how='left'
        )
        .merge(
            index_pct[['year_week', 'pct_very_low', 'pct_low', 'pct_moderate', 'pct_high', 'pct_very_high']],
            on='year_week', how='left'
        )
        .merge(week_start, on='year_week', how='left')
    )

    result['green_hours_pct'] = result['pct_very_low'] + result['pct_low']
    result['record_count'] = result['record_count'].astype('int64')
    result['year'] = result['year'].astype('int64')
    result['week_number'] = result['week_number'].astype('int64')

    result = result.sort_values('year_week').reset_index(drop=True)

    print(f"sustainability_reports: {len(result)} rows")
    return result
