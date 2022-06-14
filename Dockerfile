FROM apache/airflow:2.2.0

USER root

ENV DEBIAN_FRONTEND=noninteractive \
    TERM=linux \
    AIRFLOW_GPL_UNIDECODE=yes

RUN apt-get -y update \
 && apt-get -y install python-pip libpq-dev postgresql-client python3-dev python3-distutils python3-apt