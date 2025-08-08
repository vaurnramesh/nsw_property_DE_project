FROM apache/airflow:2.10.5-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

WORKDIR $AIRFLOW_HOME
# COPY ./google /opt/airflow/google