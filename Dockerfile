FROM apache/airflow
ADD requirements.txt .
RUN pip install -r requirements.txt