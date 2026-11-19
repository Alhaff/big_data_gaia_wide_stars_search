# Образ курсу + Python-пакети з requirements.txt
FROM apache/spark:4.2.0

# spark-submit, pyspark без повного шляху (у базовому образі /opt/spark/bin немає в PATH)
ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

USER root
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
USER spark
