import os
import pandas as pd
import numpy as np
from typing import Optional
from faker import Faker
from src.logger import logger
from src.config import AnalysisConfig


class DataSynthesizer:
    """
    A utility class for generating, managing, and storing synthetic campaign data for testing and development purposes.
    """
    def __init__(self, generate_df_on_init: bool = True):
        """
        Initializes the synthesizer, optionally generating synthetic data immediately.

        Args:
            generate_df_on_init (bool): If True, generates synthetic campaign data during initialization.
        """
        self.config = AnalysisConfig()
        self.faker = Faker()
        self.channel_name_choices = ["Search", "Display", "Social", "Video"]  # Available channel names to choose from
        self.campaign_name_choices = self.get_mock_campaign_names()  # Available campaign names to choose from
        self.generated_data: Optional[pd.DataFrame] = None
        if generate_df_on_init:
            self.generate_campaign_data()

    def get_mock_campaign_names(self, max_variations: int = 100) -> list[str]:
        """
        Generates a list of realistic, formatted campaign names using the Faker library.

        Args:
            max_variations (int): The maximum number of names to generate.

        Returns:
            list[str]: A list of formatted company-style campaign names.
        """
        amount = min(max_variations, self.config.NUM_SYNTHETIC_ROWS)
        # We do some replace() cleanup to achieve a uniform look for each name
        return [self.faker.company().replace(",", "").replace(".", "").replace(" ", "_").replace("-", "_")
                for _ in range(amount)]

    def generate_campaign_data(self):
        """
        Generates synthetic campaign data and stores it in the instance as a pandas DataFrame.

        The DataFrame includes columns for campaign IDs, names, channels, impressions,
        clicks, conversions, cost, and dates.
        """
        num_rows = self.config.NUM_SYNTHETIC_ROWS
        campaign_ids = np.arange(num_rows)
        # We define the value for each field through vectorized numpy arrays
        campaign_names = np.random.choice(self.campaign_name_choices, size=num_rows, replace=True)
        channels = np.random.choice(self.channel_name_choices, size=num_rows, replace=True)
        impressions = np.random.randint(1000, 500000, size=num_rows)
        clicks = np.array([np.random.randint(10, imp + 1) for imp in impressions])
        conversions = np.array([np.random.randint(1, cl + 1) for cl in clicks])
        cost_eur = np.round(np.random.uniform(100, 10000, num_rows), 2)

        # For random dates we select a start date with a reasonable lower bound to pick offsets from
        start_date = pd.Timestamp("2000-01-01")
        today = pd.Timestamp("now").normalize()
        days_since_start_date = (today - start_date).days
        dates = pd.to_datetime(np.random.randint(0, days_since_start_date, size=num_rows), unit="D", origin=start_date)
        # The reference specifies the dates to be follow the string format 'YYYY-MM-DD'
        dates = dates.strftime('%Y-%m-%d')
        self.generated_data = pd.DataFrame({
            "campaign_id": campaign_ids,
            "campaign_name": campaign_names,
            "channel": channels,
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
            "cost_eur": cost_eur,
            "date": dates
        })
        logger.info(f"🔁 Updated the generated data with {num_rows} rows of synthetic campaign data.")

    def save_to_disk(self):
        """
        Saves the DataFrame in the state of 'self.generated_data' to a CSV in the specified directory,
        """
        if self.generated_data is None:
            raise ValueError("No synthetic data has been generated yet, since initializing the class.")
        try:
            os.makedirs(self.config.SYNTHETIC_DIRECTORY, exist_ok=True)
            self.generated_data.to_csv(self.config.SYNTHETIC_DATA_PATH, index=False)
            logger.info(
                f"✅ Stashed {len(self.generated_data)} of {self.config.NUM_SYNTHETIC_ROWS} rows to {self.config.SYNTHETIC_DATA_PATH}")
            logger.info(
                f"📦 Approximate size: {os.path.getsize(self.config.SYNTHETIC_DATA_PATH) / (1024 * 1024):.2f} MB")
        except (OSError, PermissionError) as e:
            raise RuntimeError(f"Error creating directory or saving data: {e}")
        except AttributeError as e:
            raise TypeError(f"Generated data is not in the expected format: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error occurred while saving generated data: {e}")


if __name__ == "__main__":
    DataSynthesizer(generate_df_on_init=True).save_to_disk()
