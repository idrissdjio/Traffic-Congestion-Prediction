# Use official Spark with Python
FROM apache/spark-py:latest

# Set working directory
WORKDIR /app

# Install Python and dependencies
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

# Reinstall numpy and pandas to ensure compatibility
RUN pip install --no-cache-dir numpy==1.23.5 pandas==1.5.3

# Copy source code and requirements file
COPY ./src /app/src
COPY ./requirements.txt /app/requirements.txt

# Copy the machine_learning folder into the Docker image
COPY ./machine_learning /app/machine_learning

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Create missing Ivy cache directory
RUN mkdir -p /opt/spark/.ivy2/cache && chmod -R 777 /opt/spark/.ivy2

# Pre-download required Spark & Kafka dependencies
RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.0/spark-token-provider-kafka-0-10_2.12-3.4.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Ensure proper permissions
RUN chmod -R 777 /opt/spark/jars /opt/spark/.ivy2

# Add user ID to /etc/passwd if not present
RUN echo "185:x:185:0:anonymous uid:/opt/spark:/bin/false" >> /etc/passwd

# Switch back to non-root user
USER 185

# Run Spark with required packages
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", "/app/src/streaming_processing/kafka_consumer.py"]