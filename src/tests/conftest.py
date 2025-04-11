import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from src.pipeline import CampaignAnalysisPipeline


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def pipeline(spark):
    logger = MagicMock()
    return CampaignAnalysisPipeline(spark=spark, logging_instance=logger)
