FROM apache/airflow:3.1.5-python3.11

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags /opt/airflow/dags
COPY utils /opt/airflow/utils

