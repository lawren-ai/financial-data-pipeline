import great_expectations as gx
from great_expectations.core import ExpectationConfiguration
from great_expectations.dataset import PandasDataset
import pandas as pd
from datetime import datetime, timedelta


def create_context():
    context = gx.get_context()
    return context

def validate_ohclv_data(df: pd.DataFrame) -> dict:
    dataset = PandasDataset(df)
    results = {'success': True, 'errors': [], 'warnings': []}

    required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
    expectations = []

    try:
        # check for all required columns
        for col in required_columns:
            result = dataset.expect_column_to_exist(col)
            expectations.append(result)
            if not result.success:
                results['errors'].append(f"Missing required  column: {col}")
                results['success'] = False
        
        # data type checks
        dataset.expect_column_values_to_be_of_type('ticker', 'object')
        dataset.expect_column_values_to_be_of_type('open', 'float64')
        dataset.expect_column_values_to_be_of_type('high', 'float64')
        dataset.expect_column_values_to_be_of_type('low', 'float64') 
        dataset.expect_column_values_to_be_of_type('close', 'float64')
        dataset.expect_column_values_to_be_of_type('volume', 'int64')   
        dataset.expect_column_values_to_be_of_type('volume', 'int64')

        # null value checks
        for col in ['open', 'high', 'low', 'close', 'volume']:
            result = dataset.expect_column_values_to_not_be_null(col)
            if not result.success:
                null_pct = result.result['unexpected_percent']
                results['errors'].append(f"{col} has {null_pct:.2f}% null values")
                results['success'] = False

            
        # Price range validation
        for price_col in ['open', 'high', 'low', 'close']:
            result = dataset.expect_column_values_to_be_between(
                price_col, min_value=0.01, max_value=100000
            )

            if not result.success:
                results['errors'].append(f"{price_col} contains unreasonable values")
                results['success'] = False

        
        # OHLC relationship checks
        # High should be >= Open, Low, Close
        if not df[df['high'] < df['open']].empty:
            results['errors'].append("High < Open in some records")
            results['success'] = False
        
        if not df[df['high'] < df['close']].empty:
            results['errors'].append('High < close in some records')
            results['success'] = False

        if not df[df['high'] < df['low']].empty:
            results['errors'].append("High < Low in some records")
            results['success'] = False

        # Low should be <= Open, High, Close
        if not df[df['low'] < df['open']].empty:
            results['errors'].append("Low > Open in some records")
            results['success'] = False

        if  not df[df['low'] > df['close']].empty:
            results['errors'].append("Low  > Close in some records")
            results['success'] = False

        # volume checks
        result = dataset.expect_column_values_to_be_between('volume', min_value=0)
        if not result.success:
            results['errors'].append("Volume contains negative values")
            results['success'] = False   

        # check for low volumes
        median_volume = df['volume'].median()
        low_volume_count = len(df[df['volume'] < median_volume * 0.01]) 

        if low_volume_count > len(df) * 0.1:
            results['warnings'].append(f"{low_volume_count} records with suspiciously low volume")  


        # data/timestamp validation
        result = dataset.expect_column_values_to_not_be_null('data')
        if not result.success:
            results['errors'].append("Null dates detected")
            results['success'] = False
        
        # check for future dates
        today = datetime.now().date()
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
            future_dates = df[df['date'] > today]
            if not future_dates.empty:
                results['errors'].append(f"{len(future_dates)} records with future dates")
                results['success'] = False
            
        # completeness check
        expected_ticker_count = df['ticker'].nunique()
        actual_ticker_count = len(df)

        # Each ticker should have exactly one record for today
        ticker_counts = df.groupby('ticker').size()
        duplicates = ticker_counts[ticker_counts > 1]
        if not duplicates.empty:
            results['warnings'].append(f"{len(duplicates)} tickers have duplicate records")

        # outlier detection 
        # flag extreme price movements (> 50% change in a day)
        df['price_change_pct'] = abs(df['close'] - df['open']) / df['open'] * 100
        extreme_moves = df[df['price_change_pct'] > 50]
        if not extreme_moves.empty:
            results['warnings'].append(
            f"{len(extreme_moves)} records with >50% price movement")

        # ticker format validation
        invalid_tickers = df[~df['ticker'].str.match(r'^[A-Z]{1,5}$')]
        if not invalid_tickers.empty:
            results['warnings'].append(f"{len(invalid_tickers)} tickers with non_standard format")

    except Exception as e:
        results['success'] = False
        results['errors'].append(f"Validation error: {str(e)}")

    return results


def validate_options_data(df: pd.DataFrame) -> dict:
    dataset = PandasDataset(df)

    results = {'success': True, 'errors': [], 'warnings': []}

    required_columns = [
        'ticker', 'expiration', 'strike', 'option_type', 'bid',
        'ask', 'last', 'volume', 'open_interest', 'implied_volatility'
    ]

    try:
        for col in required_columns:
            result = dataset.expect_column_to_exist(col)
            if not result.success:
                results['errors'].append(f"Missing column: {col}")
                results['success'] = False

            # option type validation
            result = dataset.expect_column_values_to_be_in_set(
                'option_type', ['call', 'put']
            )

            if not result.success:
                results['errors'].append("Invalid option types detected")
                results['success'] = False

            # strike price validation
            result = dataset.expect_column_values_to_be_between('strike', min_value=0.01)
            if not result.success:
                results['errors'].append("Invalid strike prices")
                results['success'] = False
            
            # bid-ask spread validation
            df['spread'] = df['ask'] - df['bid']
            invalid_spreads = df[df['spread'] < 0]
            if not invalid_spreads.empty:
                results['errors'].append("Neative bid-ask spreads detected")
                results['success'] = False
            
            # implied volatility range check
            result = dataset.expect_column_values_to_be_between(
                'implied_volatility', min_value=0, max_value=5
            )
            if not result.success:
                results['warnings'].append("Extreme implied volatility values detected")

            # Expiration date validation
            df['expiration'] = pd.to_datetime(df['expiration']).dt.date
            today = datetime.now().date()
            past_expirations = df[df['expiration'] < today]
            if not past_expirations.empty:
                results['errors'].append("Expired options in dataset")
                results['success'] = False

    except Exception as e:
        results['success'] = False
        results['errors'].append(f"Options validation error: {str(e)}")

    return results

def validate_economic_data(data: dict) -> dict:
    results = {'success': True, 'errors': [], 'warnings': []}

    try:
        required_indicators = ['DFF', "T10Y2Y", "UNRATE"]

        for indicator in required_indicators:
            if indicator not in data or not data[indicator]:
                results['warnings'].append(f"Missing data for {indicator}")

        # validate data freshness
        for indicator, series in data.items():
            if not series:
                continue

            latest_date = max(series.keys())
            age_days = (datetime.now() - datetime.fromisoformat(latest_date)).days

            if age_days > 90:
                results['warnings'].append(
                    f"{indicator} data is {age_days} days old"
                )

    except Exception as e:
        results['success'] = False
        results['errors'].append(f"Economic data validation error: {str(e)}")

    return results


def generate_validation_report(validation_results: dict, data_type: str) -> str:
    report = f"\n{'='*60}\n"
    report += f"Data Validation REport: {data_type}\n"
    report += f"TImestamp: {datetime.now()}"
    report += f"{'='*60}\n\n"

    if validation_results['success']:
        report += " VALIDATION SUCCESSFUL\n\n"
    else:
        report += "VALIDATION FAILED\n\n"

    if validation_results['errors']:
        report += "ERRORS:\n"
        for error in validation_results['errors']:
            report += f" - {error}\n"
        report += "\n"

    if validation_results['warnings']:
        report += "WARNINGS:\n"
        for warning in validation_results['warnings']:
            report += f" - {warning}\n"
        report += "\n"

    return report















        





        


