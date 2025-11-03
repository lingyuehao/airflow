FROM apache/airflow:3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
 && ln -sf /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/default-java \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --chown=airflow:root requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data
COPY --chown=airflow:root data/ /opt/airflow/data/

USER airflow
