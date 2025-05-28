FROM apache/airflow:2.6.0-python3.8
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libaio1 \
    wget \
    unzip \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    libffi-dev \
    libssl-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client
RUN mkdir -p /opt/oracle && cd /opt/oracle && \
    wget --no-check-certificate https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    unzip instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    rm -f instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    mv instantclient_19_23 instantclient && \
    echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

# Set Oracle environment variables
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
ENV PATH=$ORACLE_HOME:$PATH

# Create Airflow directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config && \
    chown -R airflow:root /opt/airflow

USER airflow
WORKDIR /opt/airflow

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Install required Python packages (avoid conflict versions)
RUN pip install --no-cache-dir \
    apache-airflow==2.6.0 \
    apache-airflow-providers-oracle==3.3.0 \
    cx_Oracle==8.3.0 \
    openmetadata-ingestion[oracle]==1.3.3 \
    sqlalchemy==1.4.53 \
    alembic==1.12.0 \
    pydantic==1.10.12 \
    python-dateutil==2.8.2 \
    psycopg2-binary \
    redis

