from pyspark.sql import SparkSession
from src.pipeline import CampaignAnalysisPipeline
from src.config import AnalysisConfig
from src.logger import logger


def run_analysis():
    """
    A wrapper method for running a full cycle of the CampaignAnalysisPipeline() class iteratively.
    """
    config = AnalysisConfig()
    spark = SparkSession.builder \
        .appName(config.APP_NAME) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("🚀 Spark session started.")
    pipe = CampaignAnalysisPipeline(spark=spark, logging_instance=logger)
    input_df = pipe.load_from_disk(config.SYNTHETIC_DATA_PATH)
    if input_df:
        analyzed_df = pipe.analyze(input_df)
        pipe.perform_reporting(analyzed_df)
    spark.stop()
    logger.info("✅ Analysis completed.")


if __name__ == "__main__":
    run_analysis()
