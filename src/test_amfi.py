import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.connectors.amfi import AmfiConnector
from src.core.logging import logger

def test_amfi():
    connector = AmfiConnector()
    logger.info("Starting AMFI test run...")

    try:
        # Fetch last 6 months
        logger.info("Phase 1: Fetching Bronze data (last 6 months)...")
        result = connector.fetch()
        raw_path = result['raw_path']
        logger.info(f"Records fetched: {len(result['records'])}")

        # Clean
        logger.info("Phase 2: Cleaning to Silver...")
        silver_df = connector.clean(raw_path)
        logger.info(f"Silver shape: {silver_df.shape}")
        if not silver_df.empty:
            print("\nSilver Sample:")
            print(silver_df[['date', 'avg_aum_crore', 'net_inflow_crore']].head())

        # Normalize
        logger.info("Phase 3: Normalizing to Gold...")
        gold_df = connector.normalize(silver_df)
        logger.info(f"Gold shape: {gold_df.shape}")
        if not gold_df.empty:
            print("\nGold Sample:")
            print(gold_df[['metric_name', 'date', 'value']].head(10))

            # Upsert
            logger.info("Phase 4: Upserting to Gold storage...")
            connector.save_gold(gold_df)
            logger.info("Upsert complete.")
        else:
            logger.warning("Gold DataFrame is empty.")

        logger.info("AMFI test run COMPLETED.")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    test_amfi()
