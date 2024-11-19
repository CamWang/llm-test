"""
Main processing module for loan approval system
"""

from pyspark.sql import SparkSession
from validate import validate_data_integrity
from evaluate_credit import evaluate_credit_history, calculate_credit_score
from risk_assessment import assess_income_risk, assess_employment_risk, calculate_overall_risk_score
from decision_engine import make_loan_decision, calculate_loan_terms
from constants import DB_CONFIG, ENV_LOCAL

def process_loan_applications(framework_context, atlas, config, logger, session, context, dataframes):
    """
    Main processing function for loan applications
    Returns: DataFrame with approved loans
    """
    try:
        env = config.env
        logger.info("Starting loan application processing")

        # Get input dataframes
        application_df = dataframes["loan_application_frame"]
        income_df = dataframes["income_employment_frame"]
        credit_df = dataframes["credit_history_frame"]
        assets_df = dataframes["assets_liabilities_frame"]
        loan_history_df = dataframes["loan_history_frame"]

        # Data validation
        valid_data = validate_data_integrity(
            application_df, income_df, credit_df, assets_df, logger
        )
        logger.info("Data validation completed")

        # Credit assessment
        credit_assessed = evaluate_credit_history(credit_df, loan_history_df)
        credit_scored = calculate_credit_score(credit_assessed)
        logger.info("Credit assessment completed")

        # Risk assessment
        income_risk = assess_income_risk(income_df)
        employment_risk = assess_employment_risk(income_df)
        risk_scored = calculate_overall_risk_score(
            valid_data.join(credit_scored, 'customer_id')
            .join(income_risk, 'application_id')
            .join(employment_risk, 'application_id')
        )
        logger.info("Risk assessment completed")

        # Make decisions
        decisions = make_loan_decision(risk_scored)
        final_results = calculate_loan_terms(decisions)
        logger.info("Decision making completed")

        # Filter approved loans and select relevant columns
        approved_loans = final_results.filter(
            col('decision') == 'APPROVED'
        ).select(
            'application_id',
            'customer_id',
            'loan_amount',
            'approved_amount',
            'interest_rate',
            'risk_category',
            'credit_score',
            'annual_income',
            'loan_term_months'
        )

        # Write results to database
        db_config = DB_CONFIG.get(env, DB_CONFIG[ENV_LOCAL])
        approved_loans.write.mode('overwrite').format(
            db_config['format']
        ).save(db_config['path'])

        logger.info("Loan processing completed successfully")
        return approved_loans

    except Exception as e:
        logger.error(f"Error in loan processing: {str(e)}")
        raise