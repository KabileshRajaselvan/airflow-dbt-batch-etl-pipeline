def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_daily_revenue_empty_when_no_marts(client):
    resp = client.get("/api/marts/daily-revenue")
    assert resp.status_code == 200
    assert resp.json() == []


def test_daily_revenue_returns_seeded_rows(client, seeded_marts):
    resp = client.get("/api/marts/daily-revenue?limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 2
    assert body[0]["metric_date"] == "2026-01-02"  # most recent first


def test_product_engagement_returns_seeded_rows(client, seeded_marts):
    resp = client.get("/api/marts/product-engagement")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["product_id"] == "product_1"


def test_segment_summary_returns_seeded_rows(client, seeded_marts):
    resp = client.get("/api/marts/segment-summary")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["segment"] == "high_value"


def test_pipeline_status_reflects_seeded_data(client, seeded_marts):
    resp = client.get("/api/marts/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["latest_metric_date"] == "2026-01-02"
    assert body["days_available"] == 2


def test_pipeline_status_empty_state(client):
    resp = client.get("/api/marts/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["latest_metric_date"] is None
    assert body["days_available"] == 0
