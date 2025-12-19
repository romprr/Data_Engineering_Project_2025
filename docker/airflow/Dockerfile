FROM apache/airflow:3.1.3

USER root
RUN apt-get update && apt-get install -y unzip curl

USER airflow
# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 
