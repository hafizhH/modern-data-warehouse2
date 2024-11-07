from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import lag, col, window
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CurrencyForecasting") \
    .config("spark.sql.warehouse.dir", "/user/warehouse") \
    .getOrCreate()

def prepare_features(currency_code):
    # Read the historical data
    df = spark.read.parquet("/user/warehouse/currency_rates") \
        .filter(col("currency") == currency_code)
    
    # Create features using window functions
    window_spec = Window.orderBy("timestamp")
    
    df = df \
        .withColumn("rate_lag_1", lag("rate", 1).over(window_spec)) \
        .withColumn("rate_lag_2", lag("rate", 2).over(window_spec)) \
        .withColumn("rate_lag_3", lag("rate", 3).over(window_spec)) \
        .withColumn("rate_lag_7", lag("rate", 7).over(window_spec)) \
        .na.drop()
    
    return df

def train_model(currency_code):
    # Prepare the data
    df = prepare_features(currency_code)
    
    # Create feature vector
    feature_cols = ["rate_lag_1", "rate_lag_2", "rate_lag_3", "rate_lag_7"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Create the pipeline
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    gbt = GBTRegressor(featuresCol="scaled_features", labelCol="rate", 
                       maxIter=100, maxDepth=5)
    
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # Split the data
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
    
    # Train the model
    model = pipeline.fit(train_data)
    
    # Evaluate the model
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="rate", predictionCol="prediction",
                                  metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    
    print(f"Root Mean Square Error (RMSE) for {currency_code}: {rmse}")
    
    # Save the model
    model.write().overwrite().save(f"/user/warehouse/models/currency_forecast_{currency_code}")
    
    return model

def forecast_currency(model, currency_code, days_ahead=7):
    # Get the most recent data
    recent_data = prepare_features(currency_code).orderBy(col("timestamp").desc()).limit(7)
    
    # Make predictions
    predictions = model.transform(recent_data)
    
    return predictions.select("timestamp", "rate", "prediction")

if __name__ == "__main__":
    # Train models for major currencies
    currencies = ["EUR", "GBP", "JPY", "AUD", "CAD"]
    
    for currency in currencies:
        print(f"Training model for {currency}...")
        model = train_model(currency)
        
        # Make forecasts
        forecasts = forecast_currency(model, currency)
        
        # Save forecasts to HDFS
        forecasts.write \
            .mode("overwrite") \
            .parquet(f"/user/warehouse/forecasts/{currency}")