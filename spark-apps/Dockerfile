# Use the official Spark base image
FROM bitnami/spark:latest

# Install Python dependencies
USER root
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install requests pandas scikit-learn numpy

# Copy the application files
COPY spark-apps/currency_fetcher.py /opt/spark-apps/
COPY spark-apps/currency_forecasting.py /opt/spark-apps/

# Set necessary environment variables
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV SPARK_DRIVER_MEMORY=2G
ENV SPARK_EXECUTOR_MEMORY=2G

# Create directory for checkpoints
RUN mkdir -p /tmp/checkpoint

# Switch back to non-root user
USER 1001

# Set the entry point
CMD ["spark-submit", "--master", "${SPARK_MASTER_URL}", "/opt/spark-apps/currency_fetcher.py"]