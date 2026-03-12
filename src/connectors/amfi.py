import urllib.request
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
import json
import os
from datetime import datetime
from src.connectors.base import BaseConnector
from src.core.storage import storage_manager
from src.core.logging import logger
from src.core.validation import validate_dataframe

NS = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'

# Mapping of (month_abbr, year) -> xlsx URL
MONTH_ABBR = {
    1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun',
    7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'
}

class AmfiConnector(BaseConnector):
    def __init__(self):
        super().__init__(sector="AMC", source_system="AMFI")
        self.base_url = "https://portal.amfiindia.com/spages"

    def _build_url(self, month: int, year: int) -> str:
        abbr = MONTH_ABBR[month]
        return f"{self.base_url}/am{abbr}{year}repo.xls"

    def _parse_xlsx_bytes(self, data: bytes) -> dict:
        """Parse AMFI XLSX bytes using namespace-agnostic logic."""
        buf = io.BytesIO(data)
        all_records = []
        with zipfile.ZipFile(buf) as z:
            available = z.namelist()

            # 1. Shared strings
            shared_strings = []
            if 'xl/sharedStrings.xml' in available:
                with z.open('xl/sharedStrings.xml') as f:
                    ss_tree = ET.parse(f)
                for si in ss_tree.findall('.//{*}si'):
                    texts = si.findall('.//{*}t')
                    shared_strings.append(''.join(t.text or '' for t in texts))

            # 2. Sheets: Only process the first sheet (aggregate industry report)
            sheet_paths = [n for n in available if 'worksheets/sheet' in n and n.endswith('.xml')]
            sheet_paths.sort() 
            
            if not sheet_paths:
                return []

            with z.open(sheet_paths[0]) as f:
                ws_tree = ET.parse(f)

            rows = {}
            for row in ws_tree.findall('.//{*}row'):
                r_num = row.get('r')
                if not r_num: continue
                r_num = int(r_num)
                cells = {}
                for c in row.findall('{*}c'):
                    r_attr = c.get('r', '')
                    col_ref = ''.join(filter(str.isalpha, r_attr))
                    v = c.find('{*}v')
                    is_el = c.find('{*}is')
                    t = c.get('t', '')
                    if is_el is not None:
                        texts = is_el.findall('.//{*}t')
                        cells[col_ref] = ''.join(tx.text or '' for tx in texts)
                    elif v is not None:
                        if t == 's' and shared_strings:
                            idx = int(v.text)
                            if idx < len(shared_strings):
                                cells[col_ref] = shared_strings[idx]
                        else:
                            cells[col_ref] = v.text
                rows[r_num] = cells

            for r_num, cells in rows.items():
                label = str(cells.get('B', '')).strip()
                if not label or label.upper() in ['SR NO', 'CATEGORY', 'SCHEME NAME']:
                    continue
                if label.isdigit(): continue
                
                import re
                label = re.sub(r'\s+', ' ', label)
                label = re.sub(r'[*]+$', '', label).strip()
                
                # Broad Aggregates to skip
                if any(x in label.upper() for x in ['SUB TOTAL', 'TOTAL A', 'TOTAL B', 'TOTAL C', 'GRAND TOTAL']):
                    continue
                
                # Metrics
                metrics = {
                    'net_inflow_crore': self._safe_float(cells.get('G')),
                    'net_aum_crore':    self._safe_float(cells.get('H')),
                    'avg_aum_crore':    self._safe_float(cells.get('I')),
                    'mobilized_crore':  self._safe_float(cells.get('E')),
                    'redemption_crore': self._safe_float(cells.get('F')),
                    'folios':           self._safe_float(cells.get('D')),
                    'schemes':          self._safe_float(cells.get('C')),
                }
                if all(v is None for v in metrics.values()): continue
                
                record = {'label': label}
                record.update(metrics)
                all_records.append(record)
        return all_records

    def _safe_float(self, val):
        try:
            return float(str(val).replace(',', '').strip())
        except:
            return None

    def fetch(self, months: list = None) -> dict:
        """
        Fetch AMFI monthly industry aggregates for the given list of (month, year) tuples.
        Defaults to last 12 months, starting from Sep 2025 backwards.
        """
        if months is None:
            # Build last 12 months starting from a known-available month
            # AMFI typically publishes with a 1-2 month lag
            months = []
            m, y = 3, 2026  # Start from Mar 2026 (current month)
            for _ in range(12):
                months.append((m, y))
                m -= 1
                if m == 0:
                    m, y = 12, y - 1

        # 1. Check for manual XLSX seeds first (priority)
        local_folder = "amfi_latest_data"
        scraped_data = []
        if os.path.exists(local_folder):
            logger.info(f"Checking for local XLSX in {local_folder}...")
            # We need to reuse our Excel parsing logic
            for filename in os.listdir(local_folder):
                if filename.endswith('.xlsx'):
                    filepath = os.path.join(local_folder, filename)
                    logger.info(f"Ingesting local AMFI XLSX: {filename}")
                    try:
                        with open(filepath, 'rb') as f:
                            xlsx_bytes = f.read()
                        records = self._parse_xlsx_bytes(xlsx_bytes)
                        if records:
                            # Try to extract year/month from filename (e.g., amjan2026repo.xlsx)
                            import re
                            match = re.search(r'am([a-z]{3})(\d{4})', filename.lower())
                            if match:
                                m_abbr, y_str = match.groups()
                                m_num = next(k for k, v in MONTH_ABBR.items() if v == m_abbr)
                                y_num = int(y_str)
                                for r in records:
                                    r['month'] = m_num
                                    r['year'] = y_num
                                    r['date'] = f"{y_num}-{m_num:02d}"
                                    r['source_url'] = f"local://{filename}"
                                scraped_data.extend(records)
                                logger.info(f"  Successfully ingested {len(records)} records for {m_abbr} {y_num}")
                    except Exception as e:
                        logger.error(f"Error reading {filename}: {e}")

        # 2. Check for manual/scraped seeds
        scraped_path = "data/bronze/AMC/amfi_latest_scraped.json"
        if os.path.exists(scraped_path):
            try:
                with open(scraped_path, 'r') as f:
                    scraped_data.extend(json.load(f))
                logger.info(f"Loaded {len(scraped_data)} scraped records from {scraped_path}")
            except Exception as e:
                logger.warning(f"Failed to load scraped data: {e}")

        results = scraped_data # Start with local/scraped data
        seen_dates = {r['date'] for r in results} if results else set()

        # 3. Fetch missing months from portal
        for (month, year) in months:
            date_str = f"{year}-{month:02d}"
            if date_str in seen_dates:
                continue

            url = self._build_url(month, year)
            logger.info(f"Fetching AMFI data from {url}")
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    raw_bytes = resp.read()  # read as raw bytes, not BytesIO

                parsed_list = self._parse_xlsx_bytes(raw_bytes)
                if parsed_list:
                    for entry in parsed_list:
                        entry['month'] = month
                        entry['year'] = year
                        entry['date'] = f"{year}-{month:02d}"
                        entry['source_url'] = url
                        results.append(entry)
                    logger.info(f"  AMFI {year}-{month:02d}: Extracted {len(parsed_list)} categories")
                else:
                    logger.warning(f"  No data found for {year}-{month:02d}")
            except Exception as e:
                logger.warning(f"  Failed to fetch {url}: {e}")

        # Save to Bronze as JSON
        os.makedirs("data/bronze/AMC", exist_ok=True)
        bronze_path = "data/bronze/AMC/amfi_industry_monthly.json"
        with open(bronze_path, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Bronze data saved to: {bronze_path}")
        return {"raw_path": bronze_path, "records": results}

    def clean(self, raw_path: str) -> pd.DataFrame:
        """Load JSON Bronze into Silver DataFrame."""
        with open(raw_path, 'r') as f:
            records = json.load(f)
        df = pd.DataFrame(records)
        if df.empty:
            return df
        df['date'] = pd.to_datetime(df['date'])
        return df

    def normalize(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """Map Silver to Gold schema."""
        gold_rows = []
        extraction_ts = datetime.now()

        for _, row in silver_df.iterrows():
            label = row.get('label', 'Unknown')
            # Categorize based on label
            subsector = 'Industry Aggregate'
            if 'GRAND TOTAL' not in label.upper() and 'SUB TOTAL' not in label.upper():
                subsector = label
                
            base = {
                'sector': 'AMC',
                'subsector': subsector,
                'entity_name': label,
                'entity_type': 'Category' if 'TOTAL' not in label.upper() else 'Aggregate',
                'geography': 'India',
                'date': row['date'].date(),
                'period_start': row['date'].date(),
                'period_end': (row['date'] + pd.offsets.MonthEnd(0)).date(),
                'frequency': 'Monthly',
                'unit': 'Crores',
                'currency': 'INR',
                'source_system': 'AMFI',
                'source_url': row.get('source_url', 'https://amfiindia.com'),
                'publication_date': None,
                'extraction_timestamp': extraction_ts,
                'revision_flag': False,
            }

            metrics = [
                ('amc.avg_aum',        'avg_aum_crore',    'Average AUM'),
                ('amc.net_aum',        'net_aum_crore',    'Net AUM'),
                ('amc.net_inflow',     'net_inflow_crore', 'Net Inflow'),
                ('amc.mobilized',      'mobilized_crore',  'Gross Mobilization'),
                ('amc.redemption',     'redemption_crore', 'Redemption'),
                ('amc.folios',         'folios',           'Number of Folios'),
                ('amc.schemes',        'schemes',          'Number of Schemes'),
            ]

            for metric_id, col, metric_label in metrics:
                val = row.get(col)
                if val is None:
                    continue
                # For folios and schemes, change unit
                unit = 'Crores'
                if metric_id in ['amc.folios', 'amc.schemes']:
                    unit = 'Count'
                
                rec = base.copy()
                rec.update({
                    'metric_name': metric_id,
                    'source_metric_label': f"{label} - {metric_label}", 
                    'value': float(val),
                    'unit': unit,
                    'raw_record_reference': f"amfi_{row['date'].strftime('%Y%m')}_{label.replace(' ', '_')}_{metric_id}",
                })
                gold_rows.append(rec)

        gold_df = pd.DataFrame(gold_rows)
        if not gold_df.empty:
            validate_dataframe(gold_df)
        return gold_df
