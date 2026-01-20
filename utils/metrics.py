import pandas as pd 
from datetime import datetime, date
from typing import Dict
from utils.db_utils import get_db_connection

def calculate_daily_metrics(date_str: str) -> Dict:
    """"
    Calculate daily pipeline metrics
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        
    Returns:
        Dictionary of metrics
    """
    target_date = pd.to_datetime(date_str).date()

    metrics = {}

    with get_db_connection() as conn:
        # collection rate
        cur = conn.cursor()

        # Expected ticker count (from stock universe)
        cur.execute('SELECT COUNT(*) FROM stock_universe WHERE is_active = true')
        expected_count = cur.fetchone()[0]

        # actually collected
        cur.execute("""
                    SELECT COUNT(DISTINCT ticker)
                    FROM ohclv_data
                    WHERE date = %s
                    """, (target_date,))
        collected_count = cur.fetchone()[0]

        metrics['collection_rate'] = (collected_count / expected_count * 100) if expected_count > 0 else 0
        metrics['tickers_collected'] = collected_count
        metrics['tickers_expected'] = expected_count
        metrics['tickers_missing'] = expected_count - collected_count

        # Data quality metrics
        cur.execute("""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(*) FILTER (WHERE open > 0 AND high > 0 AND low > 0 AND close > 0),
                        COUNT(*) FILTER (WHERE high >= open AND high >= close AND high >= low) as valid_ohlc,
                        COUNT(*) FILTER (WHERE volume >= 0) as valid_volume
                    FROM ohclv_data
                    WHERE date = %s
                    """, (target_date, ))
        
        quality = cur.fetchone()
        if quality:
            total = quality[0]
            if total > 0:
                metrics['price_validity_rate'] = quality[1] / total * 100
                metrics['ohlc_logic_rate'] = quality[2] / total * 100
                metrics['volume_validity_rate'] = quality[3] / total * 100

        # Data freshness
        cur.execute("""
            SELECT MAX(created_at)
            FROM ohclv_data
            WHERE date = %s
            """, (target_date,))
        
        last_update = cur.fetchone()[0]
        if last_update:
            freshness_minutes = (datetime.now() - last_update).total_seconds() / 60
            metrics['data_freshness_minutes'] = freshness_minutes
            metrics['sla_freshness_met'] = freshness_minutes < 15

        # Volume stats
        cur.execute("""
                SELECT 
                    AVG(volume) as avg_volume,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume) as median_volume,
                    MAX(volume) as max_volume,
                    MIN(volume) as min_volume
                FROM ohclv_data
                WHERE date = %s AND volume > 0
                """, (target_date,))
        
        vol_stats = cur.fetchone()
        if vol_stats:
            metrics['avg_volume'] = float(vol_stats[0]) if vol_stats[0] else 0
            metrics['median_volume'] = float(vol_stats[1]) if vol_stats[1] else 0

        # Price movement statistics
        cur.execute("""
                SELECT 
                    AVG(ABS((close - open) / open * 100)) as avg_price_change,
                    MAX(ABS((close - open) / open * 100)) as max_price_change,
                    COUNT(*) FILTER (WHERE ABS((close - open) / open * 100) > 5) as large_moves
                FROM ohclv_data
                WHERE date = %s AND open > 0
                """, (target_date,))
        
        price_stats = cur.fetchone()
        if price_stats:
            metrics['avg_price_change_pct'] = float(price_stats[0]) if price_stats[0] else 0
            metrics['max_price_change_pct'] = float(price_stats[1]) if price_stats[1] else 0
            metrics['large_movers_count'] = price_stats[2] or 0
        
        # validation results from quality logs
        cur.execute("""
                SELECT
                    COUNT(*) as total_validations,
                    COUNT(*) FILTER (WHERE validation_success = true) as passed,
                    SUM(error_count) as total_errors,
                    SUM(warning_count) as total_warnings
                FROM data_quality_logs
                WHERE DATE(validation_timestamp) = %s
                """, (target_date,))
        
        val_stats = cur.fetchone()
        if val_stats and val_stats[0]:
            metrics['validation_pass_rate'] = (val_stats[1] / val_stats[0] * 100) if val_stats[0] > 0 else 0
            metrics['total_validation_warnings'] = val_stats[3] or 0

        # Overall health score(0-100)
        health_components = [
            metrics.get('collection_rate', 0),
            metrics.get('validation_pass_rate', 0),
            metrics.get('price_validity_rate', 0),
            metrics.get('ohclv_logic_rate', 0)
        ]

        metrics['overall_health_score'] = sum(health_components) / len(health_components) if health_components else 0

        # SLA compliance
        sla_checks = {
            'collection_rate': metrics.get('collection_rate', 0) >= 99.0,
            'validation_pass_rate': metrics.get('validation_pass_rate', 0) >= 99.5,
            'data_freshness': metrics.get('data_freshness_minutes', 999) < 15
        }

        metrics['sla_compliance_rate'] = (sum(sla_checks.values()) / len(sla_checks) * 100)
        metrics['sla_checks_passed'] = sum(sla_checks.values())
        metrics['sla_checks_total'] = len(sla_checks)

        cur.close()

    return metrics


def get_pipeline_health_summary(days: int = 7) -> Dict:
    """
    Get pipeline health summary for last N days
    
    Args:
        days: Number of days to analyze
    
    Returns:
        Dictionary with aggregated health metrics
    """
    end_date = date.today()
    start_date = end_date - pd.Timedelta(days=days)

    summary = {
        'period_days': days,
        'start_date': start_date,
        'end_date': end_date
    }

    with get_db_connection() as conn:
        cur = conn.cursor()

        # Get average metrics over period
        cur.execute("""
                SELECT
                    AVG(metric_value) FILTER (WHERE metric_name = 'collection_rate') as avg_collection_rate,
                    AVG(metric_value) FILTER (WHERE metric_name = 'validation_pass_rate') as avg_validation_rate,
                    AVG(metric_value) FILTER (WHERE metric_name = 'data_freshness_minutes') as avg_freshness,
                    AVG(metric_value) FILTER (WHERE metric_name = 'overall_health_score') as avg_health_score,
                    COUNT(DISTINCT date) as days_with_data
                FROM pipeline_metrics
                WHERE date >= %s AND date <= %s
                 """, (start_date, end_date))
        
        result = cur.fetchone()

        if result:
            summary['avg_collection_rate'] = float(result[0]) if result[0] else 0
            summary['avg_validation_rate'] = float(result[1]) if result[1] else 0
            summary['avg_freshness_minutes'] = float(result[2]) if result[2] else 0
            summary['avg_health_score'] = float(result[3]) if result[3] else 0
            summary['days_with_data'] = result[4] or 0

        # Count failures
        cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE validation_success = false) as failed_validations,
                    COUNT(DISTINCT dag_id) as active_dags
                FROM data_quality_logs
                WHERE DATE(validation_timestamp) >= %s AND DATE(validation_timestamp) <= %s 
                    """, (start_date, end_date))
        result = cur.fetchone()
        if result:
            summary['failed_validations'] = result[0] or 0
            summary['active_dags'] = result[1] or 0

        cur.close()

    # Calculate reliability percentage
    if summary.get('days_with_data', 0) > 0:
        summary['reliability_pct'] = (summary['days_with_data'] / days * 100)
    else:
        summary['reliability_pct'] = 0

    return summary
         



        


        