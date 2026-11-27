FROM apache/spark:3.5.3

USER root

# Python-зависимости
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Автоматическая загрузка JDBC-драйверов PostgreSQL и ClickHouse со всеми зависимостями
RUN /opt/spark/bin/spark-shell \
    --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0 \
    -e ":quit" && \
    find /root/.ivy2 -name "*.jar" -exec cp {} /opt/spark/jars/ \; && \
    ln -sf /opt/spark/jars/clickhouse-jdbc-0.6.0.jar /opt/spark/jars/clickhouse-jdbc-0.6.0-all.jar

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/spark/work
