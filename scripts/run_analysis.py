from pyspark.sql import SparkSession
from src.pipeline import CampaignAnalysisPipeline
from src.config import AnalysisConfig
from src.logger import logger


def run_analysis():
    """
    A wrapper method for running a full cycle of the CampaignAnalysisPipeline() class iteratively.
    """
    config = AnalysisConfig()
    spark = SparkSession.builder.appName(config.APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("🚀 Spark session started.")
    pipe = CampaignAnalysisPipeline(spark=spark, logging_instance=logger)
    input_df = pipe.load_from_disk(config.SYNTHETIC_DATA_PATH)
    if input_df:
        df_analyzed = pipe.analyze(input_df)
        df_inefficient = pipe.perform_reporting(df_analyzed)
        pipe.save_pyspark_dataframe(
            df=df_inefficient
            if config.STORE_ONLY_INEFFICIENT_CAMPAIGNS
            else df_analyzed
        )
    spark.stop()
    logger.info("✅ Analysis completed.")


if __name__ == "__main__":
    run_analysis()
