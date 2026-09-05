import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_quality import DataQualityError, DataQualityFramework, QualityThresholds


def _good_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "metric_date": ["2026-01-01"] * 3,
            "id": ["a", "b", "c"],
            "value": [10.0, 20.0, 30.0],
        }
    )


def test_passes_on_clean_data():
    df = _good_df()
    result = DataQualityFramework.run_checks(
        df,
        required_columns=["metric_date", "id", "value"],
        unique_column="id",
        thresholds=QualityThresholds(min_row_count=1, value_range_column="value", date_column="metric_date"),
    )
    assert result.passed is True
    assert all(result.checks.values())


def test_fails_on_null_in_required_column():
    df = _good_df()
    df.loc[0, "value"] = None
    with pytest.raises(DataQualityError) as exc_info:
        DataQualityFramework.run_checks(
            df,
            required_columns=["metric_date", "id", "value"],
            unique_column="id",
            thresholds=QualityThresholds(min_row_count=1),
        )
    assert "null_check" in exc_info.value.failures


def test_fails_on_duplicate_unique_column():
    df = _good_df()
    df.loc[1, "id"] = "a"
    with pytest.raises(DataQualityError) as exc_info:
        DataQualityFramework.run_checks(
            df, required_columns=["metric_date", "id", "value"], unique_column="id", thresholds=QualityThresholds()
        )
    assert "unique_check" in exc_info.value.failures


def test_fails_on_row_count_below_threshold():
    df = _good_df()
    with pytest.raises(DataQualityError) as exc_info:
        DataQualityFramework.run_checks(
            df,
            required_columns=["metric_date", "id", "value"],
            unique_column="id",
            thresholds=QualityThresholds(min_row_count=10),
        )
    assert "row_count_check" in exc_info.value.failures


def test_fails_on_negative_value_range():
    df = _good_df()
    df.loc[0, "value"] = -5.0
    with pytest.raises(DataQualityError) as exc_info:
        DataQualityFramework.run_checks(
            df,
            required_columns=["metric_date", "id", "value"],
            unique_column="id",
            thresholds=QualityThresholds(value_range_column="value", value_min=0.0),
        )
    assert "value_range_check" in exc_info.value.failures


def test_fails_on_missing_required_column():
    df = _good_df().drop(columns=["value"])
    with pytest.raises(DataQualityError) as exc_info:
        DataQualityFramework.run_checks(
            df, required_columns=["metric_date", "id", "value"], unique_column="id", thresholds=QualityThresholds()
        )
    assert "schema_check" in exc_info.value.failures
    # null_check can't meaningfully evaluate a missing column, so it's also
    # reported as failed rather than silently skipped
    assert "null_check" in exc_info.value.failures
