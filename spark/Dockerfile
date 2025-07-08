# Use the official Spark image as the base
FROM bitnami/spark:3.5.3

# Set environment variables for Hadoop (if not already set)
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

# Install Python and pip if not already present.
USER root
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Update pip
RUN pip3 install uv
RUN uv pip install --system --no-cache-dir --upgrade pip
RUN uv pip install --system --no-cache-dir httpx

# Install Python dependencies using uv
RUN uv pip install --system --no-cache-dir -U -r /tmp/requirements.txt

# Set the working directory
WORKDIR /opt/bitnami/spark

# Add cmod for langflow
RUN mkdir /.cache/langflow
RUN chmod -R 777 /.cache/langflow

# Switch back to the Spark user if necessary
USER 1001
