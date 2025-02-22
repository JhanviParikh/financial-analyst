from src.utils.spark_utils import create_spark_session
from src.data.transaction_generator import TransactionGenerator
from src.models.risk_model import RiskModel
from src.config.settings import STREAMING_DATA_PATH, RISK_THRESHOLD
import time
import logging
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import count, avg, max, min, sum, element_at
from src.utils.analytics_utils import save_batch_analytics
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("amount_threshold_flag", DoubleType(), True),
    StructField("time_risk_score", DoubleType(), True),
    StructField("location_risk_score", DoubleType(), True),
    StructField("is_suspicious", IntegerType(), True)
])

def process_predictions(df, epoch_id):
    """Process and display predictions with monitoring metrics"""
    try:
        # Calculate risk scores first
        df = df.withColumn('amount_threshold_flag', 
            F.when(F.col('amount') > 5000, 1.0).otherwise(0.0)
        ).withColumn('time_risk_score',
            F.rand()  # Simplified for example, replace with actual logic
        ).withColumn('location_risk_score',
            F.rand()  # Simplified for example, replace with actual logic
        ).withColumn('risk_score',  # Calculate risk score
            F.when(
                (F.col('amount') > 5000) & 
                (F.col('location_risk_score') > 0.7), 
                0.9
            ).when(
                F.col('amount') > 8000,
                0.8
            ).when(
                F.col('location_risk_score') > 0.7,
                0.7
            ).when(
                F.col('time_risk_score') > 0.7,
                0.6
            ).otherwise(0.2)
        )

        # Select columns in the desired order
        df = df.select(
            'transaction_id',
            'timestamp',
            'amount',
            'transaction_type',
            'account_id',
            'location',
            'risk_score',  # Include risk_score in output
            'prediction',
            'amount_threshold_flag',
            'time_risk_score',
            'location_risk_score',
            'is_suspicious',
            'hour_of_day',
            'day_of_week',
            'amount_log',
            'amount_scaled',
            'latitude',
            'longitude'
        )

        # Debug log
        print("Columns before saving:")
        df.printSchema()
        df.show(5)

        # Save batch data for analytics
        save_batch_analytics(df, epoch_id)
        
        # Calculate monitoring metrics
        metrics = df.agg(
            count('*').alias('total_transactions'),
            sum(F.when(F.col('risk_score') > 0.7, 1).otherwise(0)).alias('suspicious_transactions'),
            avg('amount').alias('avg_amount'),
            max('amount').alias('max_amount'),
            avg('risk_score').alias('avg_confidence')
        ).collect()[0]

        # Log metrics
        logger.info(f"\nBatch {epoch_id} Metrics:")
        logger.info(f"Total Transactions: {metrics['total_transactions']:,}")
        logger.info(f"Suspicious Transactions: {metrics['suspicious_transactions']:,}")
        logger.info(f"Average Amount: ${metrics['avg_amount']:.2f}")
        logger.info(f"Max Amount: ${metrics['max_amount']:.2f}")
        logger.info(f"Average Risk Score: {metrics['avg_confidence']:.2%}")

        # Show suspicious transactions
        suspicious = df.filter(F.col('risk_score') > 0.7)
        if suspicious.count() > 0:
            logger.warning("\nSUSPICIOUS TRANSACTIONS DETECTED:")
            suspicious.show(truncate=False)

    except Exception as e:
        logger.error(f"Error in process_predictions: {e}")
        raise

def main():
    try:
        # Create checkpoint directory
        os.makedirs("/tmp/checkpoint", exist_ok=True)
        
        # Initialize Spark session
        spark = create_spark_session()
        logger.info("Spark session created")

        # Initialize transaction generator
        transaction_generator = TransactionGenerator()
        logger.info("Transaction generator initialized")

        # Initialize risk model
        risk_model = RiskModel()
        logger.info("Risk model initialized")

        # Generate smaller initial training data
        training_batch = transaction_generator.generate_batch(10)  # Even smaller batch
        training_batch.to_csv(f"{STREAMING_DATA_PATH}/training_data.csv", index=False)
        logger.info(f"Generated training data with {len(training_batch)} records")
        
        # Load training data into Spark
        training_data = spark.read.csv(
            f"{STREAMING_DATA_PATH}/training_data.csv",
            header=True,
            schema=TRANSACTION_SCHEMA
        )

        # Train the model
        logger.info("Training risk model...")
        risk_model.train(training_data)
        risk_model.save_model()
        logger.info("Risk model trained and saved")

        # Set up streaming query
        stream_data = (
            spark.readStream.schema(TRANSACTION_SCHEMA)
            .option("maxFilesPerTrigger", 1)
            .csv(STREAMING_DATA_PATH)
        )

        # Process streaming data with custom output
        query = (
            risk_model.predict(stream_data)
            .writeStream
            .foreachBatch(process_predictions)  # Use custom output function
            .outputMode("update")
            .start()
        )

        # Generate streaming data
        try:
            accumulated_transactions = None
            while True:
                # Generate new batch of transactions
                new_batch = transaction_generator.generate_batch(10, accumulate=True)
                transaction_generator.save_batch(new_batch)
                logger.info(f"Generated new batch. Total transactions: {len(new_batch)}")
                time.sleep(5)  # Wait 5 seconds between batches
        except KeyboardInterrupt:
            logger.info("Stopping the application...")
            query.stop()
            spark.stop()

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {str(e)}") 