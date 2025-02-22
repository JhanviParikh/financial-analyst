from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType
import os
from src.config.settings import MODELS_PATH
from src.features.feature_engineering import FeatureEngineer
import logging

class RiskModel:
    def __init__(self):
        self.feature_engineer = FeatureEngineer()
        self.feature_columns = [
            'amount',
            'amount_threshold_flag',
            'time_risk_score',
            'location_risk_score',
            'account_id',
            'hour_of_day',
            'day_of_week',
            'amount_log',
            'amount_scaled',
            'latitude',
            'longitude'
        ]
        self.label_column = 'is_suspicious'
        self.model_path = os.path.join(MODELS_PATH, 'risk_model')
        os.makedirs(MODELS_PATH, exist_ok=True)
        
        # Initialize the pipeline components
        self.assembler = VectorAssembler(
            inputCols=self.feature_columns,
            outputCol="features",
            handleInvalid="skip"
        )
        
        self.classifier = RandomForestClassifier(
            labelCol=self.label_column,
            featuresCol="features",
            numTrees=50,  # Reduced number of trees
            maxDepth=5,   # Reduced depth
            seed=42
        )
        
        self.pipeline = Pipeline(stages=[self.assembler, self.classifier])

    def _prepare_data(self, data):
        """Prepare data with feature engineering"""
        df = self.feature_engineer.compute_transaction_features(data)
        df = self.feature_engineer.add_location_features(df)
        
        # Convert types and handle nulls
        for column in self.feature_columns:
            if column in ['account_id']:
                df = df.withColumn(column, 
                    when(col(column).isNull(), -1)
                    .otherwise(col(column).cast(IntegerType())))
            else:
                df = df.withColumn(column, 
                    when(col(column).isNull(), 0.0)
                    .otherwise(col(column).cast(DoubleType())))
        
        df = df.withColumn(self.label_column, 
            when(col(self.label_column).isNull(), 0)
            .otherwise(col(self.label_column).cast(IntegerType())))
        
        return df

    def train(self, data):
        """Train the risk model"""
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("Starting model training...")
            
            # Optimize data partitioning
            data = data.repartition(4)
            data.cache()  # Cache the input data
            
            count = data.count()  # Materialize the cache
            logger.info(f"Training data count: {count}")
            
            # Prepare data with feature engineering
            logger.info("Preparing data...")
            prepared_data = self._prepare_data(data)
            prepared_data.cache()  # Cache the prepared data
            
            prep_count = prepared_data.count()  # Materialize the cache
            logger.info(f"Prepared data count: {prep_count}")
            
            # Create and fit the pipeline with smaller trees
            self.classifier.setNumTrees(50)  # Reduce number of trees
            self.classifier.setMaxDepth(5)   # Reduce tree depth
            
            logger.info("Fitting model...")
            self.model = self.pipeline.fit(prepared_data)
            logger.info("Model fitting complete")
            
            # Clean up
            data.unpersist()
            prepared_data.unpersist()
            
            return self.model
            
        except Exception as e:
            logger.error(f"Error during model training: {str(e)}")
            raise

    def predict(self, data):
        """Make predictions on new data"""
        prepared_data = self._prepare_data(data)
        predictions = self.model.transform(prepared_data)
        return predictions

    def save_model(self):
        """Save the trained model using Spark's native save method"""
        self.model.write().overwrite().save(self.model_path)

    def load_model(self):
        """Load a trained model using Spark's native load method"""
        from pyspark.ml import PipelineModel
        
        if os.path.exists(self.model_path):
            self.model = PipelineModel.load(self.model_path)
            return True
        return False 