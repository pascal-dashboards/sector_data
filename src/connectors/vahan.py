import asyncio
import os
# import playwright moved inside method to handle environment constraints
import pandas as pd
from datetime import datetime
import io
from src.connectors.base import BaseConnector
from src.core.storage import storage_manager
from src.core.logging import logger
from src.core.validation import validate_dataframe

class VahanConnector(BaseConnector):
    def __init__(self):
        super().__init__(sector="Auto", source_system="VAHAN")
        self.url = "https://vahan.parivahan.gov.in/vahan4dashboard/"

    async def _fetch_with_playwright(self, year: int) -> dict:
        """Fetch VAHAN data using Playwright."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("Playwright not found. Cannot fetch VAHAN data directly.")
            # If we already have a local JSON file in a known path, we could return that.
            # For the MVP, we assume the subagent has provided the JSON.
            local_json = f"data/bronze/Auto/vahan_{year}_monthwise.json"
            if os.path.exists(local_json):
                logger.info(f"Found pre-fetched JSON at {local_json}")
                return {"raw_path": local_json, "year": year}
            raise ImportError("Playwright missing and no pre-fetched data found.")

        async with async_playwright() as p:
            logger.info("Launching browser for VAHAN...")
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_page()
            
            await context.goto(self.url)
            await context.wait_for_load_state("networkidle")
            
            # Navigate to Tabular Summary if not already there
            # (Based on subagent research, we might need to click 'Main Page View' or similar)
            # For brevity and reliability in headless, we check the URL or specific selectors.
            
            logger.info(f"Setting filters for Year: {year} and Month Wise X-Axis...")
            
            # Select 'Month Wise' in X-Axis
            await context.select_option("select[name*='xaxisVar']", label="Month Wise")
            
            # Select Year
            await context.select_option("select[name*='selectedYear']", label=str(year))
            
            # Click Refresh (the button often has a specific primefaces ID or class)
            # The subagent found 'j_idt66'
            await context.click("button[id*='idt66']")
            
            # Wait for data table to update
            await context.wait_for_selector("table[id*='groupingTable']", timeout=30000)
            
            # Get the table HTML
            table_html = await context.eval_on_selector("table[id*='groupingTable']", "el => el.outerHTML")
            
            # Save to Bronze
            raw_path = storage_manager.save_bronze(
                sector=self.sector,
                filename=f"vahan_{year}_monthwise.html",
                content=table_html,
                metadata={
                    "source_url": self.url,
                    "extraction_timestamp": datetime.now().isoformat(),
                    "params": {"year": year, "xaxis": "Month Wise"}
                }
            )
            
            await browser.close()
            return {"raw_path": raw_path, "year": year}

    def fetch(self, year: int = None) -> dict:
        """Wrapper for async fetch."""
        if year is None:
            year = datetime.now().year
            
        # Run async code in sync wrapper for base class compatibility
        return asyncio.run(self._fetch_with_playwright(year))

    def clean(self, raw_path: str) -> pd.DataFrame:
        """Parse VAHAN HTML or JSON into a Silver DataFrame."""
        import json
        with open(raw_path, 'r') as f:
            if raw_path.endswith('.json'):
                records = json.load(f)
                df = pd.DataFrame(records)
            else:
                html_content = f.read()
                try:
                    dfs = pd.read_html(io.StringIO(html_content))
                    if not dfs: return pd.DataFrame()
                    df = dfs[0]
                except Exception as e:
                    logger.error(f"Failed to parse HTML table: {e}")
                    return pd.DataFrame()
            
        df.columns = [str(col).strip().upper() for col in df.columns]
        
        # Detect type of VAHAN data
        if 'MAKER' in df.columns:
            data_type = 'Maker Wise'
            id_col = 'MAKER'
        elif 'FUEL' in df.columns:
            data_type = 'Fuel Wise'
            id_col = 'FUEL'
        else:
            data_type = 'Vehicle Class'
            # Find vehicle category column
            id_col = next((c for c in df.columns if 'VEHICLE' in c or 'CATEGORY' in c), df.columns[0])
            
        df = df.rename(columns={id_col: 'dimension_value'})
        df['data_type'] = data_type
        
        # Melt month columns
        month_cols = [c for c in df.columns if c in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']]
        df_melted = df.melt(
            id_vars=['dimension_value', 'data_type'],
            value_vars=month_cols,
            var_name='month',
            value_name='registrations'
        )
        return df_melted

    def normalize(self, silver_df: pd.DataFrame, year: int) -> pd.DataFrame:
        """Map Silver columns to Gold unified schema."""
        gold_rows = []
        extraction_ts = datetime.now()
        month_map = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                     'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
        
        for _, row in silver_df.iterrows():
            # Get current month label and convert to number
            month_label = row.get('month', '').upper()
            month_num = month_map.get(month_label)
            if not month_num: 
                continue
            
            # USER REQUEST: Stop with Feb 2026
            if year >= 2026 and month_num >= 3:
                continue
                
            period_date = datetime(year, month_num, 1).date()
            
            data_type = row.get('data_type', 'Vehicle Class')
            if data_type == 'Maker Wise':
                continue # Already Skipping Maker Wise per user request
                
            metric_base = "auto.registrations"
            if data_type == 'Fuel Wise':
                metric_name = f"{metric_base}_fuel"
                subsector = "Fuel Trends"
            else:
                metric_name = f"{metric_base}_total"
                subsector = "Vehicle Segments"

            gold_rows.append({
                'sector': 'Auto',
                'subsector': subsector,
                'metric_name': metric_name,
                'source_metric_label': row['dimension_value'],
                'entity_name': row['dimension_value'],
                'entity_type': data_type,
                'geography': 'India',
                'date': period_date,
                'period_start': period_date,
                'period_end': (pd.to_datetime(period_date) + pd.offsets.MonthEnd(1)).date(),
                'frequency': 'Monthly',
                'value': self._parse_float(row['registrations']),
                'unit': 'Count',
                'currency': None,
                'source_system': 'VAHAN',
                'source_url': self.url,
                'publication_date': None,
                'extraction_timestamp': extraction_ts,
                'revision_flag': False,
                'raw_record_reference': f"vahan_{year}_{row['month']}_{row['dimension_value'].replace(' ', '_')}",
            })
        gold_df = pd.DataFrame(gold_rows)
        validate_dataframe(gold_df)
        return gold_df

    def _parse_float(self, val):
        try:
            return float(str(val).replace(',', '').strip())
        except:
            return 0.0
