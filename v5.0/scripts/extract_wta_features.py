#!/usr/bin/env python3
"""
Extract WTA features for all available seasons and save to parquet files.
This enables the consolidation script to build wta_all.parquet.
"""
import sys
sys.path.insert(0, 'backend')
from pathlib import Path
from features.registry import get_extractor
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path('data')
FEATURES_DIR = DATA_DIR / 'features'

def extract_wta_features():
    """Extract WTA features for 2023, 2024, 2026 seasons."""
    ext = get_extractor('wta', DATA_DIR)
    
    seasons = [2023, 2024, 2026]
    for season in seasons:
        print(f"\nWTA {season}:")
        try:
            df = ext.extract_all(season)
            if df is not None and len(df) > 0:
                output_path = FEATURES_DIR / f'wta_{season}.parquet'
                df.to_parquet(output_path, index=False)
                print(f"  ✓ Extracted {len(df)} rows × {len(df.columns)} cols")
                print(f"  ✓ Saved to wta_{season}.parquet")
            else:
                print(f"  ⚠ Empty result for season {season}")
        except Exception as e:
            logger.error(f"Failed to extract WTA {season}: {e}", exc_info=True)
    
    print("\nWTA extraction complete")

if __name__ == '__main__':
    extract_wta_features()
