import os
from pyspark.sql import SparkSession

def test_spark():
    # Set security properties
    os.environ['JAVA_OPTS'] = '-Djava.security.manager=allow'
    
    spark = (SparkSession.builder
             .appName("TestSpark")
             .master("local[*]")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.ui.enabled", "false")
             .config("spark.security.manager.enabled", "false")
             .getOrCreate())
    
    # Create a simple DataFrame
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Show the DataFrame
    df.show()
    
    # Stop the session
    spark.stop()

if __name__ == "__main__":
    test_spark() 