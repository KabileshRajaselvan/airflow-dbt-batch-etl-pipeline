"""Synthetic third-party "marketing/user-enrichment API" that the ETL pipeline
extracts from daily — stands in for a real vendor API (e.g. a CDP or ad
platform) without needing a real account/credentials."""
import random
from datetime import date, datetime, timezone

from fastapi import FastAPI, Query

app = FastAPI(title="Mock Enrichment API")

SEGMENTS = ["high_value", "standard", "at_risk", "new"]
CHANNELS = ["organic", "paid_search", "paid_social", "referral", "email"]
NUM_USERS = 2000


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/user-enrichment")
def user_enrichment(as_of_date: str = Query(default=str(date.today()))) -> list[dict]:
    """Deterministic-per-day synthetic enrichment records for every user."""
    rng = random.Random(as_of_date)
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "user_id": f"user_{i}",
            "segment": rng.choice(SEGMENTS),
            "ltv_score": round(rng.uniform(0, 100), 2),
            "acquisition_channel": rng.choice(CHANNELS),
            "as_of_date": as_of_date,
            "extracted_at": now,
        }
        for i in range(1, NUM_USERS + 1)
    ]
