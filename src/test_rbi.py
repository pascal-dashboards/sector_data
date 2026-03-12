import sys
import os
import pandas as pd
from datetime import datetime

# Add src to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.connectors.rbi import RbiConnector
from src.core.logging import logger

def test_rbi():
    connector = RbiConnector()
    logger.info("Starting RBI test run...")
    
    try:
        # Step 1: Fetch
        logger.info("Phase 1: Fetching Bronze data...")
        fetch_results = connector.fetch()
        raw_path = fetch_results.get('raw_path')
        if not raw_path:
            logger.error("No raw data path returned.")
            return

        logger.info(f"Bronze data found at: {raw_path}")
        
        # Step 2: Clean
        logger.info("Phase 2: Cleaning to Silver...")
        silver_df = connector.clean(raw_path)
        logger.info(f"Silver DataFrame shape: {silver_df.shape}")
        if not silver_df.empty:
            print("\nSilver Data Sample (Head):")
            print(silver_df.head())
        
        # Step 3: Normalize
        logger.info("Phase 3: Normalizing to Gold...")
        gold_df = connector.normalize(silver_df)
        logger.info(f"Gold DataFrame shape: {gold_df.shape}")
        
        if not gold_df.empty:
            print("\nGold Data Sample (Head):")
            print(gold_df.head()[['metric_name', 'date', 'value', 'source_metric_label']])
            
            # Step 4: Save Gold
            logger.info("Phase 4: Upserting to Gold storage...")
            connector.save_gold(gold_df)
            logger.info("Successfully upserted records to in-memory Gold storage.")
        else:
            logger.warning("Gold DataFrame is empty.")
            
        logger.info("RBI test run COMPLETED.")
        
    except Exception as e:
        logger.error(f"Test run failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    test_rbi()
