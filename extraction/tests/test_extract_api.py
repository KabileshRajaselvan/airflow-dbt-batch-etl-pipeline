import sys
from pathlib import Path

import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from extract_api import fetch_user_enrichment


@responses.activate
def test_fetch_user_enrichment_returns_dataframe():
    responses.add(
        responses.GET,
        "http://mock-api:8000/api/user-enrichment",
        json=[
            {
                "user_id": "user_1",
                "segment": "high_value",
                "ltv_score": 88.5,
                "acquisition_channel": "organic",
                "as_of_date": "2026-01-01",
                "extracted_at": "2026-01-01T00:00:00+00:00",
            }
        ],
        status=200,
    )

    df = fetch_user_enrichment("2026-01-01", api_base="http://mock-api:8000")

    assert len(df) == 1
    assert df.iloc[0]["user_id"] == "user_1"
    assert df.iloc[0]["segment"] == "high_value"


@responses.activate
def test_fetch_user_enrichment_raises_on_http_error():
    responses.add(
        responses.GET,
        "http://mock-api:8000/api/user-enrichment",
        json={"detail": "server error"},
        status=500,
    )

    try:
        fetch_user_enrichment("2026-01-01", api_base="http://mock-api:8000")
        assert False, "expected an exception"
    except Exception as e:
        assert "500" in str(e)
