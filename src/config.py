class AnalysisConfig:
    # Environmental logic
    APP_NAME = "CampaignAnalysis"
    DATA_DIRECTORY = "gen_data"
    REPORTING_DIRECTORY = f"{DATA_DIRECTORY}/reporting/"
    SYNTHETIC_DIRECTORY = f"{DATA_DIRECTORY}/synthetic/"
    SYNTHETIC_DATA_PATH = f"{SYNTHETIC_DIRECTORY}synthetic_campaign_data.csv"
    # Business logic
    NUM_SYNTHETIC_ROWS = 1_500_000
    CPC_INEFFICIENCY_THRESHOLD_EUR = 10.0
    MIN_ALERTING_INEFFICIENCY_PERCENTAGE = 2.5
    STORE_ONLY_INEFFICIENT_CAMPAIGNS = True
