import pytest
import os
import shutil
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from src.pipeline import CampaignAnalysisPipeline


def test_verify_columns_success(pipeline, spark):
    df = spark.createDataFrame(
        [("123", 100.0, 5)], ["campaign_id", "cost_eur", "conversions"]
    )
    pipeline.verify_columns(df, {"campaign_id", "cost_eur", "conversions"})


def test_verify_columns_missing(pipeline, spark):
    df = spark.createDataFrame([("123",)], ["campaign_id"])
    with pytest.raises(ValueError) as exc_info:
        pipeline.verify_columns(df, {"campaign_id", "cost_eur"})
    assert "Missing required columns" in str(exc_info.value)


def test_analyze_output_schema(pipeline, spark):
    df = spark.createDataFrame(
        [
            ("c1", 100.0, 5),
            ("c1", 200.0, 10),
            ("c2", 50.0, 0),
        ],
        ["campaign_id", "cost_eur", "conversions"],
    )
    result = pipeline.analyze(df)
    assert "campaign_id" in result.columns
    assert "total_cost_eur" in result.columns
    assert "total_conversions" in result.columns
    assert "cost_per_conversion_eur" in result.columns


def test_calculate_inefficiency_amount(pipeline, spark):
    df = spark.createDataFrame(
        [
            Row(cost_per_conversion_eur=10.0),
            Row(cost_per_conversion_eur=20.0),
            Row(cost_per_conversion_eur=5.0),
        ]
    )
    total, inefficient = pipeline.calculate_inefficiency_amount(df, threshold=15.0)
    assert total == 3
    assert inefficient == 1


@patch("src.pipeline.os.makedirs")
@patch("src.pipeline.datetime")
def test_save_pyspark_dataframe(
    mock_datetime, mock_makedirs, pipeline, spark, tmp_path
):
    mock_datetime.now.return_value.strftime.return_value = "20250101_120000"
    pipeline.config.REPORTING_DIRECTORY = str(tmp_path) + "/output_"
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    pipeline.save_pyspark_dataframe(df)
    mock_makedirs.assert_called_once()


def test_load_from_disk_success(pipeline, spark, tmp_path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("campaign_id,cost_eur,conversions\nc1,100,5\nc2,50,2")
    df = pipeline.load_from_disk(str(test_file))
    assert df is not None
    assert df.count() == 2


def test_load_from_disk_empty(pipeline, spark, tmp_path):
    test_file = tmp_path / "empty.csv"
    test_file.write_text("campaign_id,cost_eur,conversions\n")
    df = pipeline.load_from_disk(str(test_file))
    assert df is None


def test_perform_reporting_filters_correctly(pipeline, spark):
    pipeline.config.CPC_INEFFICIENCY_THRESHOLD_EUR = 10.0
    pipeline.config.MIN_ALERTING_INEFFICIENCY_PERCENTAGE = 30.0
    df = spark.createDataFrame(
        [
            ("c1", 100.0, 5, 20.0),  # inefficient
            ("c2", 80.0, 10, 8.0),  # efficient
        ],
        [
            "campaign_id",
            "total_cost_eur",
            "total_conversions",
            "cost_per_conversion_eur",
        ],
    )
    df_result = pipeline.perform_reporting(df, verbose=False)
    result_ids = [row["campaign_id"] for row in df_result.collect()]
    assert result_ids == ["c1"]
