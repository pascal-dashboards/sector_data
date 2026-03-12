import pandas as pd
from datetime import datetime
import json
import os
from src.connectors.base import BaseConnector
from src.core.storage import storage_manager
from src.core.logging import logger
from src.core.validation import validate_dataframe

class RbiConnector(BaseConnector):
    def __init__(self):
        super().__init__(sector="Banking & Payments", source_system="RBI-DBIE")
        self.url = "https://data.rbi.org.in/DBIE/"

    def fetch(self) -> dict:
        """Fetch RBI data from various sources (Excel datasets or pre-fetched JSON)."""
        # 1. Try ingesting fresh Excel datasets if they exist
        records = self.ingest_from_excel_datasets()
        if records:
            bronze_path = "data/bronze/Banks/rbi_datasets_ingested.json"
            os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
            # Standardize date for JSON serialization
            serializable_records = []
            for r in records:
                r_copy = r.copy()
                if isinstance(r_copy['date'], (datetime, pd.Timestamp)):
                    r_copy['date'] = r_copy['date'].strftime('%Y-%m-%d')
                serializable_records.append(r_copy)
                
            with open(bronze_path, 'w') as f:
                json.dump(serializable_records, f, indent=2)
            logger.info(f"Ingested {len(records)} records from Excel datasets to {bronze_path}")
            return {"raw_path": bronze_path, "source": "excel_datasets"}

        # 2. Fallback to pre-fetched JSONs
        files = ["data/bronze/Banks/rbi_bank_type_breakdown.json", "data/bronze/Banks/rbi_scb_business.json"]
        for f in files:
            if os.path.exists(f):
                logger.info(f"Found pre-fetched RBI JSON at {f}")
                return {"raw_path": f, "source": "json"}
        
        logger.error("No RBI data source found.")
        return {}

    def ingest_from_excel_datasets(self) -> list:
        """Parse local RBI Excel files provided in rbi_datasets/."""
        folder = "rbi_datasets"
        if not os.path.exists(folder):
            logger.warning(f"RBI datasets folder not found: {folder}")
            return []

        all_records = []
        
        # Mapping for Payment Systems (Table 45)
        t45_path = os.path.join(folder, "RBIB Table No. 45 _ Payment System Indicators.xlsx")
        if os.path.exists(t45_path):
            try:
                # Need openpyxl for engine
                df = pd.read_excel(t45_path, header=None)
                # Date is in Col 1, starting from Row 7
                for i in range(7, len(df)):
                    row = df.iloc[i]
                    date_val = str(row[1])
                    if '-' not in date_val or ' ' in date_val: continue
                    try:
                        dt = pd.to_datetime(date_val, format='%b-%Y')
                    except: continue

                    # Mapping based on analyzed column indices
                    mapping = {
                        36: ('UPI Transaction Volume', 'Lakh'),
                        37: ('UPI Transaction Value (₹ Cr)', 'Cr'),
                        30: ('IMPS Transaction Volume', 'Lakh'),
                        31: ('IMPS Transaction Value', 'Cr'),
                        34: ('NEFT Volume', 'Lakh'),
                        35: ('NEFT Value', 'Cr'),
                        16: ('RTGS Volume', 'Lakh'),
                        17: ('RTGS Value', 'Cr'),
                        50: ('Credit Card Transactions (Volume)', 'Lakh'),
                        51: ('Credit Card Transactions (Value)', 'Cr'),
                        56: ('Debit Card POS Transactions (Volume)', 'Lakh'),
                        57: ('Debit Card POS Transactions (Value)', 'Cr'),
                        98: ('Debit Card ATM Withdrawals (Volume)', 'Lakh'),
                        99: ('Debit Card ATM Withdrawals (Value)', 'Cr'),
                        62: ('PPI (Prepaid Payment Instruments) Volume', 'Lakh'),
                        63: ('PPI Value', 'Cr'),
                        # 44: ('NACH Volume', 'Lakh'), 
                        # 45: ('NACH Value', 'Cr'),
                        110: ('Credit Cards Outstanding', 'Count_Lakh'),
                        111: ('Debit Cards Outstanding', 'Count_Lakh'),
                        114: ('ATMs Deployed (On-site + Off-site)', 'Count'),
                        120: ('PoS Terminals Deployed', 'Count'),
                        # 121: ('QR Codes Deployed', 'Count')
                    }
                    for col_idx, (m_name, unit) in mapping.items():
                        if col_idx < len(row):
                            val = row[col_idx]
                            if pd.notna(val) and val != '-':
                                all_records.append({'metric': m_name, 'date': dt, 'value': self._safe_float(val), 'unit': unit})
            except Exception as e:
                logger.error(f"Error parsing Table 45: {e}")

        # Mapping for Bank Credit (Major Sectors)
        credit_path = os.path.join(folder, "Deployment of Bank Credit by Major Sectors.xlsx")
        if os.path.exists(credit_path):
            try:
                df = pd.read_excel(credit_path, header=None)
                dates_row = df.iloc[6]
                dates = []
                for j in range(3, len(df.columns)):
                    val = dates_row[j]
                    if isinstance(val, (datetime, pd.Timestamp)):
                        dates.append((j, val))
                
                sector_mapping = {
                    "Agriculture and Allied Activities": "Credit to Agriculture",
                    "Industry(Micro and Small,Medium and Large)": "Credit to Industry (Major)",
                    "Services": "Credit to Services",
                    "Personal Loans": "Gross Bank Credit - Personal Loans",
                    "Housing (Including Priority Sector Housing)": "Personal Loans (Housing)",
                    "Vehicle Loans": "Personal Loans (Vehicle)",
                    "Education": "Personal Loans (Education)",
                    "Credit Card Outstanding": "Personal Loans (Credit Cards)"
                }
                
                msme_data = {} 
                
                for i in range(len(df)):
                    label = str(df.iloc[i, 2]).strip()
                    for search_str, metric_name in sector_mapping.items():
                        if search_str == label:
                            for col_idx, dt in dates:
                                val = df.iloc[i, col_idx]
                                if pd.notna(val) and val != '-':
                                    all_records.append({'metric': metric_name, 'date': dt, 'value': self._safe_float(val), 'unit': 'Cr'})
                    
                    if "Micro and Small" == label or "Medium" == label:
                        for col_idx, dt in dates:
                            val = self._safe_float(df.iloc[i, col_idx])
                            if val:
                                msme_data[dt] = msme_data.get(dt, 0) + val

                    if "Large" == label:
                         for col_idx, dt in dates:
                            val = self._safe_float(df.iloc[i, col_idx])
                            if val:
                                all_records.append({'metric': 'Credit to Industry (Large)', 'date': dt, 'value': val, 'unit': 'Cr'})

                for dt, val in msme_data.items():
                    all_records.append({'metric': 'Credit to Industry (MSMEs)', 'date': dt, 'value': val, 'unit': 'Cr'})

            except Exception as e:
                logger.error(f"Error parsing Credit Deployment: {e}")

        return all_records

    def _safe_float(self, val):
        try:
            if isinstance(val, (int, float)): return float(val)
            # Handle cases like (1,23,456)
            s = str(val).replace(',', '').replace('(', '').replace(')', '').strip()
            if s == '-': return 0.0
            return float(s)
        except:
            return 0.0

    def clean(self, raw_path: str) -> pd.DataFrame:
        """Parse RBI JSON into a Silver DataFrame."""
        if not os.path.exists(raw_path): return pd.DataFrame()
        with open(raw_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list): # New excel-ingested format
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            return df
            
        # Legacy/Other JSON formats logic...
        if isinstance(data, dict) and "Months" in data:
            records = []
            months = data["Months"]
            for bank_group, metrics in data.items():
                if bank_group == "Months": continue
                deposits = metrics.get("Aggregate Deposits", [])
                credit = metrics.get("Bank Credit", [])
                for i, month_str in enumerate(months):
                    records.append({
                        "date": pd.to_datetime(month_str),
                        "entity_name": bank_group,
                        "metric": "banks.aggregate_deposits",
                        "value": deposits[i] if i < len(deposits) else 0
                    })
                    records.append({
                        "date": pd.to_datetime(month_str),
                        "entity_name": bank_group,
                        "metric": "banks.gross_bank_credit",
                        "value": credit[i] if i < len(credit) else 0
                    })
            return pd.DataFrame(records)
            
        return pd.DataFrame(data)

    def normalize(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """Map Silver columns to Gold unified schema."""
        if silver_df.empty: return pd.DataFrame()
        gold_rows = []
        extraction_ts = datetime.now()
        
        for _, row in silver_df.iterrows():
            metric_label = row.get('metric', 'Unknown')
            metric_mapping = {
                'UPI Transaction Volume': 'payment.upi.vol_lakh',
                'UPI Transaction Value (₹ Cr)': 'payment.upi.val_cr',
                'Credit to Agriculture': 'banks.credit.agri_cr',
                'Credit to Industry (Major)': 'banks.credit.industry_total_cr',
                'Credit to Industry (Large)': 'banks.credit.industry_large_cr',
                'Credit to Industry (MSMEs)': 'banks.credit.industry_msme_cr',
                'Credit to Services': 'banks.credit.services_cr',
                'Gross Bank Credit - Personal Loans': 'banks.credit.personal_total_cr',
                'Personal Loans (Housing)': 'banks.credit.personal_housing_cr',
                'Personal Loans (Vehicle)': 'banks.credit.personal_vehicle_cr'
            }
            unified_metric = metric_mapping.get(metric_label, f"rbi.{metric_label.lower().replace(' ', '_').replace('(', '').replace(')', '')}")
            
            gold_rows.append({
                'sector': 'Banking & Payments',
                'subsector': 'Financial Sector Indicators',
                'entity_name': 'India',
                'entity_type': 'Country',
                'geography': 'India',
                'date': row['date'].date(),
                'period_start': row['date'].date(),
                'period_end': (row['date'] + pd.offsets.MonthEnd(0)).date(),
                'frequency': 'Monthly',
                'metric_name': unified_metric,
                'source_metric_label': metric_label,
                'value': float(row['value']),
                'unit': row.get('unit', 'Unknown'),
                'currency': 'INR' if 'Value' in metric_label or 'Cr' in str(row.get('unit', '')) else None,
                'source_system': 'RBI-DBIE',
                'source_url': self.url,
                'publication_date': None,
                'extraction_timestamp': extraction_ts,
                'revision_flag': False,
                'raw_record_reference': f"rbi_{row['date'].strftime('%Y%m')}_{metric_label.replace(' ', '_')}"
            })
            
        gold_df = pd.DataFrame(gold_rows)
        if not gold_df.empty: validate_dataframe(gold_df)
        return gold_df
