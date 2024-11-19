FROM bitnami/spark:3.4.0

# Install pip dan py4j
USER root
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install pyspark py4j numpy pandas

USER 1001
