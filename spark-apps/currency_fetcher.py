from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import json
from datetime import datetime

def fetch_currency_rates():
    """Fetch current currency rates from the API"""
    api_key = "d859a94bdc40c124d4842d22"
    base_url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
    
    try:
        response = requests.get(base_url)
        data = response.json()
        
        # Extract timestamp and rates
        timestamp = datetime.fromtimestamp(data['time_last_update_unix'])
        rates = data['conversion_rates']
        
        # Convert to list of records
        records = []
        for currency, rate in rates.items():
            records.append({
                'timestamp': timestamp,
                'currency': currency,
                'rate': rate
            })
        
        return records
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return []

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CurrencyRateStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Define schema for currency data
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("currency", StringType(), True),
    StructField("rate", DoubleType(), True)
])

# Create streaming DataFrame
def create_streaming_df():
    while True:
        rates = fetch_currency_rates()
        if rates:
            df = spark.createDataFrame(rates, schema)
            
            # Write to HDFS in parquet format
            df.write \
                .mode("append") \
                .partitionBy("currency") \
                .parquet("/user/warehouse/currency_rates")
            
            # Also create a temporary view for immediate querying
            df.createOrReplaceTempView("current_rates")
            
        # Wait for 1 hour before next fetch
        time.sleep(3600)

if __name__ == "__main__":
    create_streaming_df()