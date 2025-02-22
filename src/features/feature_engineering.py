import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType

class FeatureEngineer:
    def __init__(self):
        # Time windows for aggregations
        self.windows = {
            '5min': '5 minutes',
            '1hour': '1 hour',
            '1day': '1 day'
        }

    def compute_transaction_features(self, df):
        """Compute features from transaction data"""
        # Convert timestamp to datetime and extract time features
        df = df.withColumn('timestamp', F.to_timestamp('timestamp'))
        df = df.withColumn('hour_of_day', F.hour('timestamp'))
        df = df.withColumn('day_of_week', F.dayofweek(F.to_date('timestamp')))
        
        # Compute amount-based features
        df = df.withColumn('amount_log', F.log('amount'))
        df = df.withColumn('amount_scaled', F.col('amount') / 10000.0)
        
        # Add risk-related features
        df = df.withColumn('amount_threshold_flag',
            F.when(F.col('amount') > 5000, 1.0).otherwise(0.0)
        )
        
        df = df.withColumn('time_risk_score',
            F.when(
                (F.col('hour_of_day') < 6) | (F.col('hour_of_day') > 22),
                0.8  # Higher risk during night hours
            ).otherwise(0.2)
        )
        
        # Calculate initial risk score
        df = df.withColumn('risk_score',
            F.when(
                (F.col('amount') > 5000) & (F.col('hour_of_day').isin([0,1,2,3,4,5,23])),
                0.9  # High risk for large amounts during night hours
            ).when(
                F.col('amount') > 8000,
                0.8  # High risk for very large amounts
            ).when(
                F.col('hour_of_day').isin([0,1,2,3,4,5,23]),
                0.7  # Medium-high risk during night hours
            ).when(
                F.col('amount') > 3000,
                0.6  # Medium risk for moderately large amounts
            ).otherwise(0.2)  # Base risk level
        )
        
        return df

    def add_location_features(self, df):
        """Add location-based risk features"""
        # Extract latitude and longitude
        df = df.withColumn('latitude', F.split('location', ',').getItem(0).cast('double'))
        df = df.withColumn('longitude', F.split('location', ',').getItem(1).cast('double'))
        
        # Compute location risk score
        df = df.withColumn('location_risk_score',
            F.when(F.abs('latitude') > 60, 0.8)  # High risk for extreme latitudes
            .when(F.abs('longitude') > 150, 0.7)  # High risk for extreme longitudes
            .otherwise(0.2)
        )
        
        # Update overall risk score with location factor
        df = df.withColumn('risk_score',
            F.when(
                (F.col('risk_score') > 0.7) & (F.col('location_risk_score') > 0.7),
                0.95  # Very high risk for suspicious transactions in risky locations
            ).when(
                F.col('location_risk_score') > 0.7,
                F.greatest(F.col('risk_score'), 0.7)  # Increase risk for risky locations
            ).otherwise(
                F.col('risk_score')  # Keep existing risk score
            )
        )
        
        return df

    def get_feature_columns(self):
        """Get list of feature columns"""
        return [
            'amount',
            'amount_threshold_flag',
            'time_risk_score',
            'location_risk_score',
            'account_id',  # Removed merchant_id
            'hour_of_day',
            'day_of_week',
            'amount_log',
            'amount_scaled',
            'latitude',
            'longitude'
        ] 