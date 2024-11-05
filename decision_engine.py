"""
Decision engine module for loan applications
"""

from pyspark.sql.functions import col, when
from constants import STATUS_APPROVED, STATUS_REJECTED, STATUS_REVIEW_REQUIRED

def make_loan_decision(risk_df):
    """
    Make final loan decision based on risk assessment
    Returns: DataFrame with loan decisions
    """

    return (risk_df.withColumn(
        'dti_ratio',
        (col('total_existing_debt') + col('monthly_expenses') * 12) / col('annual_income'))
            .withColumn('decision', when(((col('risk_category') == 'LOW_RISK') & (col('dti_ratio') <= 0.4)), STATUS_APPROVED)
                .when(((col('risk_category') == 'MEDIUM_RISK') & (col('dti_ratio') <= 0.35)), STATUS_REVIEW_REQUIRED)
                .otherwise(STATUS_REJECTED)))

def calculate_loan_terms(approved_df):
    """
    Calculate loan terms for approved applications
    Returns: DataFrame with loan terms
    """
    return approved_df.withColumn(
        'interest_rate',
        when(col('risk_category') == 'LOW_RISK', 0.05)
        .when(col('risk_category') == 'MEDIUM_RISK', 0.07)
        .otherwise(0.09)
    ).withColumn(
        'approved_amount',
        when(col('decision') == STATUS_APPROVED, col('loan_amount'))
        .otherwise(0)
    )