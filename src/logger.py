import logging
import sys
from src.config import AnalysisConfig


def setup_logger() -> logging.Logger:
    """
    Returns:
        logging.Logger: A pre-configured, new Logger instance.
    """
    log_instance = logging.getLogger(AnalysisConfig.APP_NAME)
    log_instance.setLevel(logging.INFO)

    # Creating the StreamHandler to direct logs to stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    log_instance.addHandler(console_handler)

    return log_instance


# The logger will be globally accessible via the sys.modules cache if depended on once
logger = setup_logger()
