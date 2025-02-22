import os
from dotenv import load_dotenv
import warnings
import pandas as pd

# Load environment variables
load_dotenv()

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

# Base paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Data paths
RAW_DATA_PATH = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_PATH = os.path.join(DATA_DIR, "processed")
STREAMING_DATA_PATH = os.path.join(DATA_DIR, "streaming")
MODELS_PATH = os.path.join(DATA_DIR, "models")
ANALYTICS_PATH = os.path.join(DATA_DIR, "analytics")

# Create directories if they don't exist and ensure proper permissions
for path in [RAW_DATA_PATH, PROCESSED_DATA_PATH, STREAMING_DATA_PATH, MODELS_PATH, ANALYTICS_PATH]:
    try:
        os.makedirs(path, exist_ok=True)
        # Ensure read/write permissions
        os.chmod(path, 0o777)
        print(f"Directory ready: {path}")  # Debug log
    except Exception as e:
        print(f"Error creating directory {path}: {e}")

# Spark Configuration
SPARK_APP_NAME = "FinancialRiskManagement"

# Risk Thresholds
RISK_THRESHOLD = 0.7

# Dashboard Configuration
DASHBOARD_HOST = "localhost"
DASHBOARD_PORT = 8050

# Model Training Configuration
TRAINING_BATCH_SIZE = 100
MODEL_FEATURES = [
    'amount',
    'amount_threshold_flag',
    'time_risk_score',
    'location_risk_score'
] 