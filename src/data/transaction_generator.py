import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from src.config.settings import STREAMING_DATA_PATH
import random

class TransactionGenerator:
    def __init__(self):
        self.transaction_types = ['PAYMENT', 'TRANSFER', 'WITHDRAWAL', 'DEPOSIT']
        os.makedirs(STREAMING_DATA_PATH, exist_ok=True)

    def generate_transaction(self):
        """Generate a single transaction"""
        amount = float(round(np.random.uniform(10, 10000), 2))
        hour = datetime.now().hour
        
        # Calculate initial risk factors
        is_large_amount = amount > 5000
        is_night_hours = hour < 6 or hour > 22
        
        # Higher chance of suspicious transactions during night hours or with large amounts
        is_suspicious = np.random.choice([0, 1], p=[0.7, 0.3]) if (is_large_amount or is_night_hours) else np.random.choice([0, 1], p=[0.95, 0.05])
        
        # Calculate initial risk score
        risk_score = 0.2  # Base risk
        if is_large_amount and is_night_hours:
            risk_score = 0.9
        elif is_large_amount:
            risk_score = 0.7
        elif is_night_hours:
            risk_score = 0.6
        
        return {
            'transaction_id': str(np.random.randint(10000, 99999)),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'amount': amount,
            'transaction_type': np.random.choice(self.transaction_types),
            'account_id': int(np.random.randint(1000, 9999)),
            'location': f"{np.random.uniform(-90, 90):.6f},{np.random.uniform(-180, 180):.6f}",
            'risk_score': risk_score,
            'prediction': float(is_suspicious),
            'amount_threshold_flag': float(is_large_amount),
            'time_risk_score': float(is_night_hours),
            'location_risk_score': 0.0,  # Will be calculated later
            'is_suspicious': int(is_suspicious)
        }

    def generate_batch(self, batch_size=10, accumulate=True):
        """Generate a batch of transactions
        
        Args:
            batch_size (int): Number of transactions to generate
            accumulate (bool): If True, add to previous transactions
        """
        # Get current transactions if accumulating
        current_transactions = []
        if accumulate:
            try:
                # Read all existing CSV files in the streaming directory
                for file in sorted(os.listdir(STREAMING_DATA_PATH)):
                    if file.endswith('.csv'):
                        file_path = os.path.join(STREAMING_DATA_PATH, file)
                        df = pd.read_csv(file_path)
                        current_transactions.append(df)
            except Exception as e:
                print(f"Error reading existing transactions: {e}")

        # Generate new transactions using generate_transaction method
        new_transactions = [self.generate_transaction() for _ in range(batch_size)]

        # Create DataFrame with new transactions
        new_df = pd.DataFrame(new_transactions)
        
        # Combine with existing transactions if accumulating
        if accumulate and current_transactions:
            all_transactions = pd.concat([pd.concat(current_transactions), new_df], ignore_index=True)
            return all_transactions
        
        return new_df

    def save_batch(self, batch, filename=None):
        """Save batch to CSV file"""
        if filename is None:
            filename = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        file_path = os.path.join(STREAMING_DATA_PATH, filename)
        batch.to_csv(file_path, index=False)
        
        # Clean up old files but keep the last N files
        self.cleanup_old_files(max_files=10)
        
        return file_path

    def cleanup_old_files(self, max_files=10):
        """Keep only the latest N files"""
        files = sorted([f for f in os.listdir(STREAMING_DATA_PATH) if f.endswith('.csv')])
        if len(files) > max_files:
            for file_to_remove in files[:-max_files]:
                try:
                    os.remove(os.path.join(STREAMING_DATA_PATH, file_to_remove))
                except Exception as e:
                    print(f"Error removing file {file_to_remove}: {e}") 