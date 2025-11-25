# Start from the official Airflow image
FROM apache/airflow:2.10.1

# Switch to root to install system dependencies if needed (optional but good practice)
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to airflow user
USER airflow

# Upgrade pip first (Crucial for speed)
RUN pip install --upgrade pip

# Copy requirements
COPY requirements.txt /requirements.txt

# Install packages with a timeout to fail fast if stuck
RUN pip install --no-cache-dir --default-timeout=100 -r /requirements.txt