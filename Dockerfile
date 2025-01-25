FROM apache/airflow:2.10.4

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

ENV PYTHONPATH=/opt/airflow/utils:$PYTHONPATH