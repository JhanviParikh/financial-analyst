import os
import sys
from pyspark.sql import SparkSession
from src.config.settings import SPARK_APP_NAME
import subprocess

def create_spark_session():
    """Create and configure Spark session"""
    # Find Java home using system command
    try:
        java_home = subprocess.check_output(['/usr/libexec/java_home']).decode().strip()
    except:
        # Fallback to common Java locations
        possible_java_homes = [
            '/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home',
            '/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home',
            '/usr/local/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home'
        ]
        java_home = next((path for path in possible_java_homes if os.path.exists(path)), None)
        if not java_home:
            raise Exception("Java 11 installation not found. Please install Java 11.")

    # Set environment variables
    os.environ['JAVA_HOME'] = java_home
    os.environ['SPARK_LOCAL_IP'] = 'localhost'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 pyspark-shell'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Create Spark session with optimized configuration
    spark = (SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3")
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8 -Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Djava.security.manager=allow")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate())
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark 