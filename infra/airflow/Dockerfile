FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary \
    sqlalchemy \
    pyspark \
    findspark \
    pyarrow \
    fastparquet

USER root

RUN ln -sf /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow