FROM eclipse-temurin:8-jdk-focal

ARG SCALA_VERSION=2.12.17
ARG SPARK_VERSION=3.4.0
ARG HADOOP_VERSION=3
ARG PROTOC_VERSION=3.6.0

ENV DEBIAN_FRONTEND=noninteractive

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    unzip \
    build-essential \
    python3 \
    python3-pip \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Maven
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && apt-get install -y --no-install-recommends maven && \
    rm -rf /var/lib/apt/lists/*

# Scala (via coursier for reliability)
RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | \
    gzip -d > /usr/local/bin/cs && \
    chmod +x /usr/local/bin/cs && \
    cs install scala:${SCALA_VERSION} scalac:${SCALA_VERSION} && \
    ln -s ~/.local/share/coursier/bin/scala /usr/local/bin/scala && \
    ln -s ~/.local/share/coursier/bin/scalac /usr/local/bin/scalac

# Spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz && \
    tar xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# protoc
RUN curl -fsSL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    -o /tmp/protoc.zip && \
    unzip /tmp/protoc.zip -d /opt/protoc && \
    rm /tmp/protoc.zip

ENV PATH=/opt/protoc/bin:$PATH

# Python dependencies
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Working directory
WORKDIR /workspace

# Default: build the project
COPY pom.xml .
RUN mvn dependency:go-offline -q 2>/dev/null || true
