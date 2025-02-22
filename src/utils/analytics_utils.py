import pandas as pd
import os
from datetime import datetime
from src.config.settings import ANALYTICS_PATH

def save_batch_analytics(df, epoch_id):
    """Save batch data for analytics"""
    try:
        # Ensure analytics directory exists
        os.makedirs(ANALYTICS_PATH, exist_ok=True)
        
        # Convert Spark DataFrame to Pandas
        pdf = df.toPandas()
        
        # Debug log
        print(f"Saving analytics with columns: {pdf.columns.tolist()}")
        print(f"Sample data:\n{pdf.head()}")
        
        # Save to CSV
        filename = f"analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(ANALYTICS_PATH, filename)
        
        # Check if file can be written
        try:
            with open(filepath, 'w') as f:
                pass
        except Exception as e:
            print(f"Cannot write to {filepath}: {e}")
            return
            
        # Save data
        pdf.to_csv(filepath, index=False)
        
        # Verify file was created
        if os.path.exists(filepath):
            print(f"Successfully saved analytics to {filepath}")
            print(f"File size: {os.path.getsize(filepath)} bytes")
        else:
            print(f"File was not created: {filepath}")
        
        # Cleanup old files
        cleanup_old_files(ANALYTICS_PATH)
        
    except Exception as e:
        print(f"Error saving analytics: {e}")
        import traceback
        print(traceback.format_exc())

def cleanup_old_files(max_files=100):
    """Clean up old analytics files"""
    files = sorted([f for f in os.listdir(ANALYTICS_PATH) if f.endswith('.csv')])
    if len(files) > max_files:
        for file_to_remove in files[:-max_files]:
            try:
                os.remove(os.path.join(ANALYTICS_PATH, file_to_remove))
            except Exception as e:
                print(f"Error removing file {file_to_remove}: {e}") 