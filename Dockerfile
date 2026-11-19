# Spark 4.2.0 image + Python packages from requirements.txt
FROM apache/spark:4.2.0

# spark-submit and pyspark without the full path (the base image does not put /opt/spark/bin in PATH)
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
USER spark
