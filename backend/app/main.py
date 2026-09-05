import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import marts
from app.core.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("etl-marts-api")

app = FastAPI(
    title="Airflow dbt Batch ETL — Marts API",
    description="Read-only API over the `marts` schema published at the end of the Airflow DAG.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(marts.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
