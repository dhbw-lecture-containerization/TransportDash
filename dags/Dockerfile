FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    gcc \
    build-essential \
    libgeos-dev \
    libproj-dev \
    proj-data \
    proj-bin \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

RUN mkdir -p /opt/airflow/dags

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root *.py /opt/airflow/dags/

COPY --chown=airflow:root assets/geography /opt/airflow/assets/geography
