FROM apache/spark:3.5.6

USER root

WORKDIR /app

COPY kafka_stream.py /app/kafka_stream.py

RUN mkdir -p /app/checkpoint \
    && curl -fL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar \
    && chown -R 185:185 /app

USER 185

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[1]", \
     "--driver-memory", "384m", \
     "--executor-memory", "384m", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6", \
     "/app/kafka_stream.py"]
