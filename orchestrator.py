"""
orchestrator.py
----------------
Single-entry pipeline runner. Runs all four sector connectors
in sequence, saves to Bronze → Silver → Gold.

Usage:
    python3 orchestrator.py [--year YYYY] [--months N]
"""

import sys, os, argparse, json, traceback
import pandas as pd
from datetime import datetime

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.connectors.amfi import AmfiConnector
from src.connectors.rbi import RbiConnector
from src.connectors.vahan import VahanConnector
from src.core.logging import logger


def run_amfi():
    connector = AmfiConnector()
    logger.info("=== AMFI (AMC/Mutual Funds) ===")
    result = connector.fetch()
    raw_path = result['raw_path']
    silver_df = connector.clean(raw_path)
    gold_df = connector.normalize(silver_df)
    if not gold_df.empty:
        connector.save_gold(gold_df)
        logger.info(f"AMFI: Extracted {len(gold_df)} Gold rows across all categories")
    return gold_df

def run_rbi():
    connector = RbiConnector()
    logger.info("=== RBI/DBIE (Banks) ===")
    result = connector.fetch()
    raw_path = result.get('raw_path')
    if not raw_path: return None
    silver_df = connector.clean(raw_path)
    gold_df = connector.normalize(silver_df)
    if not gold_df.empty:
        connector.save_gold(gold_df)
        logger.info(f"RBI: Extracted {len(gold_df)} Gold rows")
    return gold_df

def run_vahan(year: int):
    connector = VahanConnector()
    logger.info(f"=== VAHAN (Auto) - Year {year} ===")
    
    # Process all available vahan files (Total, Maker, Fuel)
    gold_dfs = []
    vahan_dir = "data/bronze/Auto"
    for filename in os.listdir(vahan_dir):
        if str(year) in filename and filename.endswith('.json'):
            raw_path = os.path.join(vahan_dir, filename)
            logger.info(f"  Cleaning {filename}...")
            silver_df = connector.clean(raw_path)
            gold_df = connector.normalize(silver_df, year)
            gold_dfs.append(gold_df)
    
    if not gold_dfs:
        logger.error("VAHAN: No data files found.")
        return None
        
    combined_gold = pd.concat(gold_dfs, ignore_index=True)
    connector.save_gold(combined_gold)
    logger.info(f"VAHAN: Total {len(combined_gold)} Gold rows extracted")
    return combined_gold

def generate_dashboard_data(all_dfs):
    logger.info("Creating dashboard_data.json...")
    combined = pd.concat([df for df in all_dfs if df is not None], ignore_index=True)
    if combined.empty: return
    
    combined['date'] = pd.to_datetime(combined['date'])
    combined = combined.sort_values(['metric_name', 'entity_name', 'date'])
    
    cards = []
    # Group by Unique Metric + Entity combination
    for (metric_name, entity_name), group in combined.groupby(['metric_name', 'entity_name']):
        # Slice to last 13 months
        group_13m = group.tail(13)
        latest = group_13m.iloc[-1]
        prev = group_13m.iloc[-2] if len(group_13m) > 1 else latest
        
        # Sanitize values (JSON doesn't support NaN)
        metric_val = float(latest['value'])
        if pd.isna(metric_val): metric_val = 0
        
        val_series = [float(v) for v in group_13m['value']]
        val_series = [0 if pd.isna(v) else v for v in val_series]
        
        # Calculate trend based on the 13-month window
        trend = 0
        if prev['value'] != 0 and not pd.isna(prev['value']):
            trend = ((metric_val - float(prev['value'])) / float(prev['value'])) * 100
            
        card = {
            "id": f"{metric_name}_{entity_name}".replace(".", "_").replace(" ", "_"),
            "sector": latest['sector'],
            "subsector": latest['subsector'],
            "title": latest['source_metric_label'],
            "subtitle": f"{latest['entity_name']} ({latest['frequency']})",
            "metric": metric_val,
            "unit": latest['unit'],
            "trend": round(trend, 2) if not pd.isna(trend) else 0,
            "trendColor": "green" if trend >= 0 else "red",
            "labels": [d.strftime('%b %y') for d in group_13m['date']],
            "values": val_series,
            "color": "#10b981" if latest['sector'] == "Auto" else ("#059669" if latest['sector'] == "Banking & Payments" else "#047857")
        }
        cards.append(card)
        
    os.makedirs("data", exist_ok=True)
    # 1. Standard JSON export
    with open("data/dashboard_data.json", 'w') as f:
        json.dump(cards, f, indent=2)
    
    # 2. JS variable export (Bypasses CORS for local file access)
    with open("data/dashboard_data.js", 'w') as f:
        f.write("window.dashboardData = ")
        json.dump(cards, f, indent=2)
        f.write(";")
        
    logger.info(f"Dashboard data generated with {len(cards)} metrics (JSON + JS)")

def main():
    parser = argparse.ArgumentParser(description='Macro-Financial Pipeline Orchestrator')
    parser.add_argument('--year', type=int, default=2024, help='Year for VAHAN data')
    parser.add_argument('--sectors', nargs='+', default=['amfi', 'rbi', 'vahan'], choices=['amfi', 'rbi', 'vahan'])
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("MACRO-FINANCIAL PIPELINE - EXHAUSTIVE MODE")
    logger.info("=" * 60)

    results = []
    if 'amfi' in args.sectors: results.append(run_amfi())
    if 'rbi' in args.sectors: results.append(run_rbi())
    if 'vahan' in args.sectors:
        for vahan_year in [2024, 2025, 2026]:
            results.append(run_vahan(vahan_year))

    generate_dashboard_data(results)
    logger.info("=" * 60)
    logger.info(f"Completed at: {datetime.now().isoformat()}")


if __name__ == '__main__':
    main()
