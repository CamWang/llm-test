"""
Constants and configuration values for the loan approval system
"""

# Environment settings
ENV_LOCAL = "local"
ENV_DEV = "dev"
ENV_PROD = "prod"

# Credit score thresholds
MIN_CREDIT_SCORE = 650
GOOD_CREDIT_SCORE = 750
EXCELLENT_CREDIT_SCORE = 800

# Income thresholds (annual)
MIN_INCOME = 30000.00
PREFERRED_INCOME = 80000.00

# Employment stability
MIN_EMPLOYMENT_YEARS = 1
PREFERRED_EMPLOYMENT_YEARS = 3

# Debt ratios
MAX_DTI_RATIO = 0.43  # Debt-to-Income ratio
MAX_LTV_RATIO = 0.80  # Loan-to-Value ratio

# Risk score weights
CREDIT_SCORE_WEIGHT = 0.35
INCOME_WEIGHT = 0.25
EMPLOYMENT_WEIGHT = 0.20
ASSETS_WEIGHT = 0.20

# Risk categories
RISK_LOW = "LOW"
RISK_MEDIUM = "MEDIUM"
RISK_HIGH = "HIGH"

# Status codes
STATUS_APPROVED = "APPROVED"
STATUS_REJECTED = "REJECTED"
STATUS_REVIEW_REQUIRED = "REVIEW_REQUIRED"

# Required fields for validation
REQUIRED_FIELDS = {
    'loan_application_frame': ['application_id', 'customer_id', 'loan_amount', 'loan_term_months'],
    'income_employment_frame': ['annual_income', 'employment_status', 'years_employed'],
    'credit_history_frame': ['credit_score', 'total_existing_debt'],
    'assets_liabilities_frame': ['monthly_expenses', 'savings_balance']
}

# Database configuration
DB_CONFIG = {
    'local': {
        'format': 'parquet',
        'path': '/local/data/approved_loans'
    },
    'dev': {
        'format': 'parquet',
        'path': '/dev/data/approved_loans'
    },
    'prod': {
        'format': 'parquet',
        'path': '/prod/data/approved_loans'
    }
}