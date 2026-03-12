import sys
import os
sys.path.append(os.getcwd())

from src.connectors.rbi import RbiConnector
from src.core.logging import logger

def test_rbi_excel_ingestion():
    print("--- Starting RBI Excel Ingestion Test ---")
    connector = RbiConnector()
    
    print("Calling fetch()...")
    result = connector.fetch()
    
    if "raw_path" in result:
        print(f"Success! Bronze file saved at: {result['raw_path']}")
        
        print("Cleaning and Normalizing...")
        silver_df = connector.clean(result['raw_path'])
        print(f"Silver DataFrame Shape: {silver_df.shape}")
        
        gold_df = connector.normalize(silver_df)
        print(f"Gold DataFrame Shape: {gold_df.shape}")
        
        if not gold_df.empty:
            print("\nSample Gold Data (Top 10):")
            print(gold_df[['date', 'metric_name', 'value', 'unit']].head(10).to_string())
            
            print("\nUnique Metrics Found:")
            print(gold_df['source_metric_label'].unique())
        else:
            print("Warning: Gold DataFrame is empty.")
    else:
        print("Failed: fetch() did not return a raw_path.")

if __name__ == "__main__":
    test_rbi_excel_ingestion()
