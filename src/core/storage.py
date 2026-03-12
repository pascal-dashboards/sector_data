import os
import json
import pandas as pd
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
from datetime import datetime
import hashlib
from typing import Optional, Any

class StorageManager:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.bronze_path = os.path.join(base_path, "bronze")
        self.silver_path = os.path.join(base_path, "silver")
        self.gold_path = os.path.join(base_path, "gold")
        self.db_path = os.path.join(self.gold_path, "macro_data.duckdb")
        
        # Initialize DuckDB or Fallback
        if HAS_DUCKDB:
            self.con = duckdb.connect(self.db_path)
            self._init_gold_tables()
        else:
            print("WARNING: duckdb not found. Using in-memory fallback for Gold layer.")
            self.con = None
            self.memory_gold = pd.DataFrame()

    def _init_gold_tables(self):
        """Initializes the unified Gold table if it doesn't exist."""
        if self.con:
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS gold_metrics (
                    sector VARCHAR,
                    subsector VARCHAR,
                    metric_name VARCHAR,
                    source_metric_label VARCHAR,
                    entity_name VARCHAR,
                    entity_type VARCHAR,
                    geography VARCHAR,
                    date DATE,
                    period_start DATE,
                    period_end DATE,
                    frequency VARCHAR,
                    value DOUBLE,
                    unit VARCHAR,
                    currency VARCHAR,
                    source_system VARCHAR,
                    source_url VARCHAR,
                    publication_date DATE,
                    extraction_timestamp TIMESTAMP,
                    revision_flag BOOLEAN,
                    raw_record_reference VARCHAR,
                    raw_row_hash VARCHAR PRIMARY KEY
                )
            """)

    def save_bronze(self, sector: str, filename: str, content: Any, metadata: dict):
        """Saves raw data to the Bronze layer."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sector_dir = os.path.join(self.bronze_path, sector, timestamp)
        os.makedirs(sector_dir, exist_ok=True)
        
        file_path = os.path.join(sector_dir, filename)
        
        if isinstance(content, (dict, list)):
            with open(file_path, 'w') as f:
                json.dump(content, f, indent=4)
        elif isinstance(content, str):
            with open(file_path, 'w') as f:
                f.write(content)
        elif isinstance(content, bytes):
            with open(file_path, 'wb') as f:
                f.write(content)
        else:
            # Fallback for pandas/other formats if needed
            pass
            
        metadata_path = os.path.join(sector_dir, "metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
            
        return file_path

    def save_silver(self, sector: str, table_name: str, df: pd.DataFrame):
        """Saves cleaned data to the Silver layer."""
        sector_dir = os.path.join(self.silver_path, sector)
        os.makedirs(sector_dir, exist_ok=True)
        
        file_path = os.path.join(sector_dir, f"{table_name}.parquet")
        try:
            df.to_parquet(file_path, index=False)
        except ImportError:
            # Fallback to CSV if parquet dependencies are missing
            file_path = os.path.join(sector_dir, f"{table_name}.csv")
            df.to_csv(file_path, index=False)
        return file_path

    def upsert_gold(self, df: pd.DataFrame):
        """Upserts data into the unified Gold table."""
        # Ensure column order matches
        expected_cols = [
            'sector', 'subsector', 'metric_name', 'source_metric_label',
            'entity_name', 'entity_type', 'geography', 'date',
            'period_start', 'period_end', 'frequency', 'value',
            'unit', 'currency', 'source_system', 'source_url',
            'publication_date', 'extraction_timestamp', 'revision_flag',
            'raw_record_reference', 'raw_row_hash'
        ]
        
        # Calculate row hash if not present
        if 'raw_row_hash' not in df.columns:
            df['raw_row_hash'] = df.apply(
                lambda row: hashlib.md5(str(row.to_dict()).encode()).hexdigest(), 
                axis=1
            )
            
        if self.con:
            # Register the dataframe as a temporary view
            self.con.register('new_data', df[expected_cols])
            # Upsert logic
            self.con.execute("""
                INSERT OR REPLACE INTO gold_metrics 
                SELECT * FROM new_data
            """)
        else:
            # In-memory fallback
            if self.memory_gold.empty:
                self.memory_gold = df[expected_cols]
            else:
                self.memory_gold = pd.concat([self.memory_gold, df[expected_cols]]).drop_duplicates(subset=['raw_row_hash'])
        
    def query_gold(self, query: str):
        if self.con:
            return self.con.execute(query).fetchdf()
        else:
            # Very basic query support for in-memory fallback
            return self.memory_gold

storage_manager = StorageManager()

