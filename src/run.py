import os
import sys
import multiprocessing
import signal
import time

def run_flask():
    """Run the Flask dashboard"""
    from src.dashboard.app import app
    app.run(debug=True, use_reloader=False, port=8050)

def run_spark():
    """Run the Spark application"""
    os.environ['PYTHONUNBUFFERED'] = '1'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 pyspark-shell'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    from src.main import main
    main()

def cleanup(processes):
    """Clean up processes"""
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join(timeout=1)
            if process.is_alive():
                process.kill()

def monitor_processes(processes):
    """Monitor processes and their logs"""
    while any(p.is_alive() for p in processes):
        for p in processes:
            if p.is_alive():
                print(f"{p.name} is running...")
        time.sleep(5)

if __name__ == "__main__":
    # Enable better multiprocessing support
    multiprocessing.set_start_method('spawn')
    
    processes = []
    try:
        # Start Flask in a separate process
        flask_process = multiprocessing.Process(target=run_flask, name="Flask")
        flask_process.start()
        processes.append(flask_process)
        print("Flask dashboard started")
        
        # Start Spark in another process
        spark_process = multiprocessing.Process(target=run_spark, name="Spark")
        spark_process.start()
        processes.append(spark_process)
        print("Spark application started")
        
        # Monitor processes
        monitor_processes(processes)
            
    except KeyboardInterrupt:
        print("\nShutting down services...")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        cleanup(processes)
        print("All services stopped") 