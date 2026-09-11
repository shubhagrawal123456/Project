FROM apache/airflow:2.9.3-python3.11

USER root
COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
