FROM apache/airflow:2.10.4-python3.11

USER root
# libpq-dev is essential for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Ensure the local bin is in PATH
ENV PATH="${PATH}:/home/airflow/.local/bin"

WORKDIR /opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Copy and install requirements (Removed --user flag)
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your project files
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root utils /opt/airflow/utils