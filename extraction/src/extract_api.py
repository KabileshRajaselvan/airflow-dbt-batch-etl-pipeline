"""Extraction from the mock third-party enrichment API."""
import os

import pandas as pd
import requests


def fetch_user_enrichment(as_of_date: str, api_base: str | None = None) -> pd.DataFrame:
    api_base = api_base or os.environ.get("MOCK_API_BASE", "http://localhost:8091")
    resp = requests.get(f"{api_base}/api/user-enrichment", params={"as_of_date": as_of_date}, timeout=30)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())
