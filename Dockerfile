FROM apache/airflow:2.7.1

USER root

# 1. procps (ps command), Java aur Curl install karein
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Fix: JAVA_HOME ko dynamically set karein taaki amd64 aur arm64 dono par chale
# Ye command sahi java path dhoond kar use environment mein set kar degi
RUN ln -s $(readlink -f /usr/bin/java | sed "s:/bin/java::") /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# 2. Spark Jars download karein
RUN mkdir -p /home/airflow/spark_jars && \
    curl -L -o /home/airflow/spark_jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /home/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3. Python libraries install karein
RUN pip install --no-cache-dir pyspark==3.5.1 boto3 psutil