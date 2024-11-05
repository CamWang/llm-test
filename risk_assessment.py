"""
Risk assessment module for loan applications
"""

from pyspark.sql.functions import col, when, expr
from constants import (
    CREDIT_SCORE_WEIGHT, INCOME_WEIGHT, EMPLOYMENT_WEIGHT,
    ASSETS_WEIGHT, MIN_EMPLOYMENT_YEARS, PREFERRED_INCOME
)

def assess_income_risk(income_df):
    """
    Assess risk based on income and employment stability
    Returns: DataFrame with income risk assessment
    """
    return income_df.withColumn(
        'income_risk_score',
        when(
            (col('annual_income') >= PREFERRED_INCOME) &
            (col('employment_status') == 'Full-time') &
            (col('years_employed') >= MIN_EMPLOYMENT_YEARS),
            100
        ).when(
            (col('employment_status') == 'Full-time') &
            (col('years_employed') >= MIN_EMPLOYMENT_YEARS),
            80
        ).otherwise(60)
    )

def assess_employment_risk(income_df):
    """
    Assess risk based on employment factors
    Returns: DataFrame with employment risk assessment
    """
    return income_df.withColumn(
        'employment_risk_score',
        when(
            (col('employment_status') == 'Full-time') &
            (col('years_employed') >= 5),
            100
        ).when(
            (col('employment_status') == 'Full-time') &
            (col('years_employed') >= 2),
            80
        ).when(
            col('employment_status') == 'Self-employed',
            70
        ).otherwise(50)
    )

def calculate_overall_risk_score(application_df):
    """
    Calculate overall risk score combining all factors
    Returns: DataFrame with final risk score
    """

    return application_df.withColumn(
        'risk_score',
        (col('credit_score') * CREDIT_SCORE_WEIGHT +
         col('income_risk_score') * INCOME_WEIGHT +
         col('employment_risk_score') * EMPLOYMENT_WEIGHT +
         col('asset_risk_score') * ASSETS_WEIGHT)
    ).withColumn(
        'risk_category',
        when(col('risk_score') >= 80, 'LOW_RISK')
        .when(col('risk_score') >= 60, 'MEDIUM_RISK')
        .otherwise('HIGH_RISK')
    )