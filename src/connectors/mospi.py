import requests
import pandas as pd
from datetime import datetime
from src.connectors.base import BaseConnector
from src.core.storage import storage_manager
from src.core.logging import logger
from src.core.validation import validate_dataframe

class MospiConnector(BaseConnector):
    def __init__(self):
        super().__init__(sector="Economy", source_system="MoSPI")
        self.base_url = "https://esankhyiki.mospi.gov.in/api"
        # Note: In a production system, these would be in a config file
        self.endpoints = {
            "cpi_index": f"{self.base_url}/cpi/getCPIIndex"
        }

    def fetch(self) -> dict:
        """Fetch CPI data from MoSPI eSankhyiki API."""
        logger.info("Fetching CPI data from MoSPI...")
        payload = {
            "revised": "0",  # 0 for New Series, 1 for Old Series
            "index_type": "1" # 1 for All India
        }
        
        try:
            response = requests.post(self.endpoints["cpi_index"], json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Save to Bronze
            raw_path = storage_manager.save_bronze(
                sector=self.sector,
                filename="cpi_index_raw.json",
                content=data,
                metadata={
                    "source_url": self.endpoints["cpi_index"],
                    "extraction_timestamp": datetime.now().isoformat(),
                    "payload": payload
                }
            )
            
            return {"raw_path": raw_path}
        except Exception as e:
            logger.error(f"Failed to fetch MoSPI data: {str(e)}")
            raise

    def clean(self, raw_path: str) -> pd.DataFrame:
        """Process raw JSON into a cleaned Silver DataFrame."""
        import json
        with open(raw_path, 'r') as f:
            raw_data = json.load(f)
            
        # The MoSPI API response typically has a 'data' key or similar
        # Based on exploration, assuming it's a list of records
        records = raw_data.get('data', raw_data) 
        df = pd.DataFrame(records)
        
        # Clean column names (lower case, snake_case)
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Standardize dates
        # MoSPI dates often come as 'Month-Year' or similar; needs robust parsing
        # For the MVP, we assume ISO-like or standard month/year columns exist
        if 'month' in df.columns and 'year' in df.columns:
            df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
        
        return df

    def normalize(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """Map Silver columns to Gold unified schema."""
        gold_rows = []
        extraction_ts = datetime.now()
        
        for _, row in silver_df.iterrows():
            # Example mapping for CPI Headline
            if 'description' in row and 'General Index' in row['description']:
                gold_rows.append({
                    'sector': 'Economy',
                    'subsector': 'Inflation',
                    'metric_name': 'economy.cpi_headline',
                    'source_metric_label': row.get('description', 'CPI General'),
                    'entity_name': 'India',
                    'entity_type': 'Country',
                    'geography': 'India',
                    'date': row['date'].date(),
                    'period_start': row['date'].date(),
                    'period_end': (row['date'] + pd.offsets.MonthEnd(1)).date(),
                    'frequency': 'Monthly',
                    'value': float(row.get('index_value', row.get('value', 0))),
                    'unit': 'Index',
                    'currency': None,
                    'source_system': 'MoSPI',
                    'source_url': self.endpoints['cpi_index'],
                    'publication_date': None, # MoSPI doesn't always provide this in API
                    'extraction_timestamp': extraction_ts,
                    'revision_flag': False,
                    'raw_record_reference': f"mospi_cpi_{row.get('id', '')}",
                })
        
        gold_df = pd.DataFrame(gold_rows)
        
        # Validate before returning
        validate_dataframe(gold_df)
        
        return gold_df
