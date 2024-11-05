"""
Main entry point for the loan approval system
Example usage: python run_loan_approval.py --env local --date 20241031
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
import logging
from process_loan import process_loan_applications
from constants import ENV_LOCAL, ENV_DEV, ENV_PROD


def create_spark_session(app_name="LoanApprovalSystem"):
    """
    Create and configure Spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()


def main():
    """
    Main entry point for the loan approval system
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Loan Approval System')
    parser.add_argument('--env', choices=[ENV_LOCAL, ENV_DEV, ENV_PROD],
                        default=ENV_LOCAL, help='Execution environment')
    parser.add_argument('--date', type=str,
                        default=datetime.now().strftime('%Y%m%d'),
                        help='Processing date (YYYYMMDD)')
    args = parser.parse_args()

    # Setup logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('loan_approval.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting loan approval process for date: {args.date}")

    try:
        # Create Spark session
        spark = create_spark_session()
        config = {
            'env': 'local',
            'date': '20241031'
        }

        # Load input data
        dataframes = {
            "fdp027_loan_application": spark.read("fdp027_loan_application"),
            "fdp027_income_employment": spark.read(
                "fdp027_income_employment"
            ),
            "fdp027_credit_history": spark.read(
                "fdp027_credit_history"
            ),
            "fdp027_assets_liabilities": spark.read(
                "fdp027_assets_liabilities"
            ),
            "fdp027_loan_history": spark.read(
                "fdp027_loan_history"
            )
        }
        logger.info("Data loaded successfully")

        # Process loan applications
        results = process_loan_applications(
            framework_context=config,
            atlas=None,  # Mock atlas service
            config=None,  # Mock config service
            logger=logger,
            session=spark,
            context=None,  # Mock context
            dataframes=dataframes
        )

        # Show results
        logger.info("Approved loans summary:")
        results.show()

        # Get some statistics
        total_approved = results.count()
        total_amount = results.select("approved_amount").sum()

        logger.info(f"Total approved applications: {total_approved}")
        logger.info(f"Total approved amount: ${total_amount:,.2f}")

    except Exception as e:
        logger.error(f"Error in loan approval process: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()