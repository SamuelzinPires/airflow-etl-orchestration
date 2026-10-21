FROM apache/airflow:3.3.1

RUN pip install --no-cache-dir pandera==0.34.0 pytest==9.1.1
