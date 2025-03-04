FROM apache/airflow:2.4.3
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /airflow/airflowsftp
COPY --chown=airflow:airflow . /airflow
COPY requirements.txt /

ENV PYTHONPATH=$PYTHONPATH:/airflow

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt



