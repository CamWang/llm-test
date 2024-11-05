"""
Data validation module for loan applications
"""

from pyspark.sql.functions import col, when, isnull, count
from constants import REQUIRED_FIELDS, MAX_DTI_RATIO

def validate_required_fields(df, table_name, logger):
    """
    Validate that all required fields are present and not null
    Returns: DataFrame with valid records only
    """
    required = REQUIRED_FIELDS.get(table_name, [])

    # Check for null values in required fields
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in required])

    for field in required:
        df = df.filter(col(field).isNotNull())

    logger.info(f"Validated required fields for {table_name}")
    return df

def calculate_dti_ratio(income_df, liabilities_df):
    """
    Calculate Debt-to-Income ratio
    Returns: DataFrame with DTI ratio
    """
    return income_df.join(
        liabilities_df,
        'application_id'
    ).withColumn(
        'dti_ratio',
        col('monthly_expenses') / (col('annual_income') / 12)
    )

def validate_data_integrity(application_df, income_df, credit_df, assets_df, logger):
    """
    Comprehensive data integrity check
    Returns: Tuple of valid DataFrames
    """
    try:
        # Validate each table
        valid_apps = validate_required_fields(application_df, 'loan_application', logger)
        valid_income = validate_required_fields(income_df, 'income_employment', logger)
        valid_credit = validate_required_fields(credit_df, 'credit_history', logger)
        valid_assets = validate_required_fields(assets_df, 'assets_liabilities', logger)

        # Join validation
        valid_data = valid_apps.join(
            valid_income,
            'application_id'
        ).join(
            valid_credit,
            'customer_id'
        ).join(
            valid_assets,
            'application_id'
        )

        return valid_data

    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise