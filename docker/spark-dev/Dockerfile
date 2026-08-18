# Local Spark runtime: real JDK, real Spark, real Delta.
#
# This container is the reason this project can claim its code executes rather
# than merely compiles. Development and tests run here, so Databricks quota is
# spent on validation runs instead of on iteration.
FROM python:3.11-slim-bookworm

# Spark needs a JVM. procps supplies `ps`, which Spark's scripts call and which
# slim images omit — its absence produces a confusing startup failure.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      procps \
      curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Keep Spark's chatter down; INFO on every micro-batch buries test output.
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

WORKDIR /work

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-fetch the Delta JARs at build time. Without this the first Spark session
# downloads them from Maven, which is slow and fails entirely when offline.
RUN python -c "\
from pyspark.sql import SparkSession; \
import delta; \
b = SparkSession.builder.appName('warm-jars') \
  .config('spark.jars.packages','io.delta:delta-spark_2.12:3.2.1') \
  .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
  .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
s = b.getOrCreate(); s.stop()" \
 && rm -rf /work/spark-warehouse /work/metastore_db /work/derby.log

CMD ["sleep", "infinity"]
