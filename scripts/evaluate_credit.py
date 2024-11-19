"""
Credit assessment module for loan applications
"""

from pyspark.sql.functions import col, when, expr
from pyspark.sql.window import Window
from constants import (
    MIN_CREDIT_SCORE, GOOD_CREDIT_SCORE, EXCELLENT_CREDIT_SCORE,
    RISK_LOW, RISK_MEDIUM, RISK_HIGH
)

def evaluate_credit_history(credit_df, loan_history_df):
    """
    Evaluate credit history and past loan performance
    Returns: DataFrame with credit risk assessment
    """
    # Analyze loan history
    loan_performance = loan_history_df.withColumn(
        'past_performance_score',
        when(col('days_past_due_max') == 0, 100)
        .when(col('days_past_due_max') <= 30, 70)
        .when(col('days_past_due_max') <= 60, 40)
        .otherwise(0)
    )

    # Combine with credit score
    return credit_df.join(
        loan_performance,
        'customer_id'
    ).withColumn(
        'credit_risk_level',
        when(
            (col('credit_score') >= EXCELLENT_CREDIT_SCORE) &
            (col('past_performance_score') >= 70),
            RISK_LOW
        ).when(
            (col('credit_score') >= GOOD_CREDIT_SCORE) &
            (col('past_performance_score') >= 40),
            RISK_MEDIUM
        ).otherwise(RISK_HIGH)
    )

def calculate_credit_score(credit_df):
    """
    Calculate final credit score considering multiple factors
    Returns: DataFrame with calculated credit score
    """
    return credit_df.withColumn(
        'final_credit_score',
        when(col('bankruptcies') > 0, col('credit_score') * 0.5)
        .when(col('late_payments_count') > 2, col('credit_score') * 0.8)
        .otherwise(col('credit_score'))
    )