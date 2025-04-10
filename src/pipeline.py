import os
import traceback
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from typing import Optional
from datetime import datetime
from .config import AnalysisConfig


class CampaignAnalysisPipeline:
    """
    A pipeline for analyzing campaign performance using PySpark.

    This class loads campaign data, verifies structure, performs aggregation and analysis,
    identifies inefficient campaigns, logs insights, and exports results to disk.
    """
    def __init__(self, spark: SparkSession, logging_instance=None):
        """
        Initializes the pipeline with a Spark session and optional logging instance.

        Args:
            spark (SparkSession): The active Spark session.
            logging_instance (optional): Logger instance to use. If None, uses default logger.
        """
        self.config = AnalysisConfig()
        self.spark = spark
        # We either attach the input logging instance to the state, or refer to the sys.modules cache in 'logger.py'
        if logging_instance:
            self.logger = logging_instance
        else:
            from .logger import logger
            self.logger = logger

    @staticmethod
    def verify_columns(df: DataFrame, required_cols: set[str]) -> None:
        """
        Verifies that a DataFrame contains the required columns.

        Args:
            df (DataFrame): The DataFrame to check.
            required_cols (set[str]): A set of column names that must be present.

        Raises:
            ValueError: If any required columns are missing.
        """
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

    def save_pyspark_dataframe(self, df: DataFrame) -> None:
        """
        Saves the input DataFrame as a CSV file in a timestamped output directory.

        Args:
            df (DataFrame): The PySpark DataFrame to store as CSV.

        Raises:
            RuntimeError: If there is an error writing the file or creating the directory.
            TypeError: If the DataFrame is not properly structured.
        """
        dir_out = self.config.REPORTING_DIRECTORY + datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            os.makedirs(dir_out, exist_ok=True)
            df.coalesce(1).write.option("header", True).mode("overwrite").csv(dir_out)
        except (OSError, PermissionError) as e:
            raise RuntimeError(f"Error creating directory or saving data: {e}")
        except AttributeError as e:
            raise TypeError(f"Reporting data is not in the expected format: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error occurred while saving reporting data: {e}")

    def load_from_disk(self, path: str) -> Optional[DataFrame]:
        """
        Loads campaign data from a CSV file into a DataFrame.

        Args:
            path (str): The path to the CSV file.

        Returns:
            Optional[DataFrame]: The loaded DataFrame, or None if the file is empty or unreadable.
        """
        self.logger.info(f"📥 Loading data from {path} ...")
        try:
            df = self.spark.read.option("header", "true").csv(path, inferSchema=True)
            if df.rdd.isEmpty():
                self.logger.warning(f"CSV file read successfully, but it's empty.")
                return None
            self.logger.info(f"📄 DataFrame fetched from disk.")
            return df
        except AnalysisException as e:
            self.logger.critical(f"Failed to read CSV from '{path}': {e}")
            return None
        except Exception as e:
            self.logger.critical(f"[UNEXPECTED] Something went wrong while reading CSV: {e}")
            traceback.print_exc()
            return None

    def analyze(self, df: DataFrame) -> DataFrame:
        """
        Analyzes campaign performance by aggregating total costs and conversions.

        Args:
            df (DataFrame): The input DataFrame with raw campaign data.

        Returns:
            DataFrame: A new DataFrame containing aggregated metrics and cost per conversion.
        """
        self.logger.info("📊 Analyzing campaign performance ...")
        self.verify_columns(df=df, required_cols={"campaign_id", "cost_eur", "conversions"})
        result_df = df.groupBy("campaign_id").agg(
            f.sum("cost_eur").alias("total_cost_eur"),
            f.sum("conversions").alias("total_conversions")
        ).withColumn(
            "cost_per_conversion_eur",
            f.when(f.col("total_conversions") > 0, f.col("total_cost_eur") / f.col("total_conversions")).otherwise(None)
        )
        return result_df

    def perform_reporting(self, df: DataFrame) -> None:
        """
        Identifies inefficient campaigns, logs metrics, previews top offenders,
        and saves the results to disk.

        Args:
            df (DataFrame): The analyzed DataFrame containing performance metrics.
        """
        threshold = self.config.CPC_INEFFICIENCY_THRESHOLD_EUR
        percent_alert = self.config.MIN_ALERTING_INEFFICIENCY_PERCENTAGE
        self.verify_columns(df=df, required_cols={"cost_per_conversion_eur"})
        # Determine which campaigns are inefficient
        agg_counts = df.select(
            f.count("*").alias("total"),
            f.sum(
                f.when(f.col("cost_per_conversion_eur") > threshold, 1).otherwise(0)
            ).alias("inefficient")
        ).first()
        count_total = agg_counts['total']
        count_inefficient = agg_counts['inefficient']
        percent_inefficient = (count_inefficient / count_total) * 100 if count_total else 0.0
        df_inefficient = df.filter(df.cost_per_conversion_eur > threshold)
        self.logger.info(f"📢 Showing campaigns with cost_per_conversion_eur > {threshold} EUR")
        # We log the 10 most inefficient campaigns using top-K sort
        df_inefficient.orderBy(f.col("cost_per_conversion_eur").desc()).limit(10).show(truncate=False)
        self.logger.info(
            f"👀️ {count_inefficient}/{count_total} campaigns are inefficient ({percent_inefficient:.2f}%)")
        if percent_inefficient < percent_alert:
            self.logger.info(f"👍 Inefficiency percentage is tolerable (below {percent_alert}%)")
        else:
            # Here we could perform an escalation, like sending an e-mail, or lighting the beacons to call for help
            self.logger.info(f"👎 Oh no! The inefficiency percentage exceeds the set ceiling of {percent_alert}%")
        self.logger.info(f"💾 Writing result to a new directory in {self.config.REPORTING_DIRECTORY}")
        self.save_pyspark_dataframe(df=df_inefficient if self.config.STORE_ONLY_INEFFICIENT_CAMPAIGNS else df)
