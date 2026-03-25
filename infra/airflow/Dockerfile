FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    curl \
    wget \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -s "$JAVA_HOME_PATH" /usr/lib/jvm/java-17 || true

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy \
    pyspark==3.5.1 \
    findspark \
    pyarrow \
    fastparquet \
    apache-airflow-providers-apache-spark==4.0.0