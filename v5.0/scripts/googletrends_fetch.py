#!/usr/bin/env python3
"""
Google Trends fetcher using pytrends.
Called as a subprocess by the TypeScript googletrends provider.

Usage:
  python3 scripts/googletrends_fetch.py --keyword="NBA" --timeframe="now 7-d" --geo="US"

Output: JSON to stdout
  {
    "interest_over_time": [ { "date": "...", "value": 42, "partial": false } ],
    "related_queries": { "top": [...], "rising": [...] },
    "trending_today": [ { "title": "...", "approx_traffic": "..." } ]
  }
"""

import sys
import json
import argparse
import time


def fetch_trends(keyword: str, timeframe: str, geo: str) -> dict:
    from pytrends.request import TrendReq
    import pandas as pd

    pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 30), retries=2, backoff_factor=0.5)

    result: dict = {
        "keyword": keyword,
        "timeframe": timeframe,
        "geo": geo,
        "interest_over_time": [],
        "related_queries": {"top": [], "rising": []},
        "trending_today": [],
    }

    # Interest over time
    try:
        pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo=geo)
        iot = pytrends.interest_over_time()
        if not iot.empty and keyword in iot.columns:
            result["interest_over_time"] = [
                {
                    "date": ts.isoformat(),
                    "value": int(row[keyword]),
                    "partial": bool(row.get("isPartial", False)),
                }
                for ts, row in iot.iterrows()
            ]
    except Exception as e:
        result["interest_over_time_error"] = str(e)

    # Related queries
    time.sleep(2)
    try:
        rq = pytrends.related_queries()
        if keyword in rq:
            top_df = rq[keyword].get("top")
            rising_df = rq[keyword].get("rising")
            if top_df is not None and not top_df.empty:
                result["related_queries"]["top"] = top_df.to_dict(orient="records")
            if rising_df is not None and not rising_df.empty:
                result["related_queries"]["rising"] = rising_df.to_dict(orient="records")
    except Exception as e:
        result["related_queries_error"] = str(e)

    # Trending searches today (US only, geo-independent)
    time.sleep(2)
    try:
        trending = pytrends.trending_searches(pn="united_states")
        if not trending.empty:
            result["trending_today"] = trending[0].tolist()[:20]
    except Exception as e:
        result["trending_today_error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", required=True)
    parser.add_argument("--timeframe", default="now 7-d")
    parser.add_argument("--geo", default="US")
    args = parser.parse_args()

    try:
        data = fetch_trends(args.keyword, args.timeframe, args.geo)
        print(json.dumps(data))
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
