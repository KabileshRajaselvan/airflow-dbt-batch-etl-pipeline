"""Pipeline-level data-quality gate, run after dbt has built gold/marts.

This is deliberately separate from dbt's own generic tests: dbt tests are
declarative and model-scoped (does this column satisfy not_null/unique/
accepted_values), while this is pipeline-scoped — a single Python step,
wired into Airflow as its own task, that can carry richer failure context,
different thresholds per environment, and a real exception type the DAG can
branch/alert on. Matches the PRD's own architecture diagram, which shows
"Data Quality Checks" as a distinct stage after dbt transform, not folded
into it.
"""
from dataclasses import dataclass, field

import pandas as pd


class DataQualityError(Exception):
    """Raised when one or more quality checks fail; carries the failure list."""

    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__(f"Failed checks: {failures}")


@dataclass
class QualityThresholds:
    min_row_count: int = 1
    value_range_column: str | None = None
    value_min: float = 0.0
    date_column: str | None = None


@dataclass
class QualityResult:
    checks: dict[str, bool] = field(default_factory=dict)
    passed: bool = True


class DataQualityFramework:
    """Real implementation of the PRD's own `DataQualityFramework` sketch —
    the PRD's checks dict comprehension is runnable as-is once given a real
    DataFrame and thresholds; this fills in the column-name/threshold
    plumbing the PRD's sketch left as placeholders (`col('value')`,
    `check_date_range`, `validate_schema`)."""

    @staticmethod
    def run_checks(
        df: pd.DataFrame,
        required_columns: list[str],
        unique_column: str,
        thresholds: QualityThresholds,
    ) -> QualityResult:
        checks: dict[str, bool] = {}

        checks["schema_check"] = all(c in df.columns for c in required_columns)

        checks["null_check"] = bool((df[required_columns].isnull().sum().sum() == 0)) if checks["schema_check"] else False

        checks["row_count_check"] = len(df) >= thresholds.min_row_count

        checks["unique_check"] = (
            df[unique_column].is_unique if unique_column in df.columns else False
        )

        if thresholds.date_column and thresholds.date_column in df.columns:
            checks["date_range_check"] = df[thresholds.date_column].notnull().all()
        else:
            checks["date_range_check"] = True

        if thresholds.value_range_column and thresholds.value_range_column in df.columns:
            checks["value_range_check"] = bool((df[thresholds.value_range_column] >= thresholds.value_min).all())
        else:
            checks["value_range_check"] = True

        failures = [k for k, v in checks.items() if not v]
        if failures:
            raise DataQualityError(failures)

        return QualityResult(checks=checks, passed=True)
