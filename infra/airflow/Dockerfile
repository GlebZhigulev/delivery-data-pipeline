FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    curl \
    wget \
    tar \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -s "$JAVA_HOME_PATH" /usr/lib/jvm/java-17 || true

ENV JAVA_HOME=/usr/lib/jvm/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV SPARK_VERSION=3.5.8
ENV HADOOP_VERSION=3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN mkdir -p /opt/spark/jars && \
    wget -q -O /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER airflow

RUN pip install --no-cache-dir --default-timeout=300 \
    "apache-airflow==2.8.1" \
    "pandas==2.1.4" \
    "numpy==1.26.4" \
    "pyarrow==12.0.1" \
    "psycopg2-binary==2.9.9" \
    "pyspark==3.5.8" \
    "apache-airflow-providers-postgres==5.10.2" \
    "apache-airflow-providers-apache-spark==4.0.0"
