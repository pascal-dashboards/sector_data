import sys
import os
import pandas as pd
from datetime import datetime

# Add src to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.connectors.mospi import MospiConnector
from src.core.logging import logger

def test_mospi():
    connector = MospiConnector()
    logger.info("Starting MoSPI test run...")
    
    try:
        # Step 1: Fetch
        logger.info("Phase 1: Fetching Bronze data...")
        fetch_results = connector.fetch()
        raw_path = fetch_results.get('raw_path')
        logger.info(f"Bronze data saved to: {raw_path}")
        
        # Step 2: Clean
        logger.info("Phase 2: Cleaning to Silver...")
        silver_df = connector.clean(raw_path)
        logger.info(f"Silver DataFrame shape: {silver_df.shape}")
        print("\nSilver Data Sample (Head):")
        print(silver_df.head())
        
        # Step 3: Normalize
        logger.info("Phase 3: Normalizing to Gold...")
        gold_df = connector.normalize(silver_df)
        logger.info(f"Gold DataFrame shape: {gold_df.shape}")
        
        if not gold_df.empty:
            print("\nGold Data Sample (Head):")
            print(gold_df.head()[['metric_name', 'date', 'value', 'unit']])
            
            # Step 4: Save Gold
            logger.info("Phase 4: Upserting to Gold storage...")
            connector.save_gold(gold_df)
            logger.info("Successfully upserted records to DuckDB.")
            
            # Verify from DuckDB
            from src.core.storage import storage_manager
            db_df = storage_manager.query_gold("SELECT * FROM gold_metrics LIMIT 5")
            print("\nQueried from Gold DuckDB:")
            print(db_df[['metric_name', 'date', 'value']])
        else:
            logger.warning("Gold DataFrame is empty. No records to save.")
            
        logger.info("MoSPI test run COMPLETED successfully.")
        
    except Exception as e:
        logger.error(f"Test run failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    test_mospi()
