# syntax=docker/dockerfile:1
FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu
USER root
COPY requirements.txt /opt/ledgerflow/requirements.txt
RUN python3 -m pip install --no-cache-dir --target=/opt/batch-deps -r /opt/ledgerflow/requirements.txt
ADD --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /opt/drivers/postgresql.jar
RUN chmod 644 /opt/drivers/postgresql.jar
ENV PYTHONPATH=/opt/ledgerflow:/opt/batch-deps:/opt/spark/python     JDBC_DRIVER_JAR=/opt/drivers/postgresql.jar     PYSPARK_PYTHON=python3     SPARK_LOCAL_IP=127.0.0.1
WORKDIR /opt/ledgerflow
COPY ledgerflow_batch ./ledgerflow_batch
COPY ledgerflow_ops ./ledgerflow_ops
COPY tests ./tests
COPY ops_tests ./ops_tests
USER spark
ENTRYPOINT ["python3", "-m", "ledgerflow_batch"]
