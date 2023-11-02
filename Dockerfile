FROM apache/airflow:slim-2.7.2-python3.10
WORKDIR /opt/airflow/
USER root
RUN apt-get update \
  && apt-get install -y \
         vim 
ENV PATH="${AIRFLOW_HOME}:${PATH}"
RUN chown -R airflow /opt/airflow
USER airflow
RUN pip install --upgrade pip
RUN pip install 'apache-airflow[celery]' \
 'apache-airflow[postgres]' \
 'apache-airflow[redis]'
RUN pip install psycopg2-binary
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt