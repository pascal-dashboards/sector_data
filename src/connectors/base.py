from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from src.core.storage import storage_manager
from src.core.logging import logger

class BaseConnector(ABC):
    def __init__(self, sector: str, source_system: str):
        self.sector = sector
        self.source_system = source_system

    @abstractmethod
    def fetch(self) -> dict:
        """Fetch raw data and save to Bronze."""
        pass

    @abstractmethod
    def clean(self, raw_data_path: str) -> pd.DataFrame:
        """Clean raw data and save to Silver."""
        pass

    @abstractmethod
    def normalize(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize Silver data to Gold schema."""
        pass

    def run_pipeline(self):
        """Execute the full ingestion pipeline for this connector."""
        logger.info(f"Starting pipeline for {self.source_system} ({self.sector})")
        try:
            # 1. Fetch
            fetch_results = self.fetch()
            raw_path = fetch_results.get('raw_path')
            
            # 2. Clean
            silver_df = self.clean(raw_path)
            self.save_silver(silver_df)
            
            # 3. Normalize
            gold_df = self.normalize(silver_df)
            self.save_gold(gold_df)
            
            logger.info(f"Successfully completed pipeline for {self.source_system}")
            return True
        except Exception as e:
            logger.error(f"Pipeline failed for {self.source_system}: {str(e)}", exc_info=True)
            return False

    def save_silver(self, df: pd.DataFrame, table_name: str = "cleaned_data"):
        return storage_manager.save_silver(self.sector, table_name, df)

    def save_gold(self, df: pd.DataFrame):
        return storage_manager.upsert_gold(df)
