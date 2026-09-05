from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    warehouse_db_host: str = "localhost"
    warehouse_db_port: int = 5432
    warehouse_db_name: str = "warehouse"
    warehouse_db_user: str = "warehouse"
    warehouse_db_password: str = "warehouse"
    cors_origins: str = "http://localhost:5220"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.warehouse_db_user}:{self.warehouse_db_password}"
            f"@{self.warehouse_db_host}:{self.warehouse_db_port}/{self.warehouse_db_name}"
        )

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
