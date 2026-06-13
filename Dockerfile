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

# Scala (download directly via coursier JAR, architecture-independent)
RUN curl -fsLo /tmp/coursier.jar https://github.com/coursier/coursier/releases/latest/download/coursier.jar && \
    java -jar /tmp/coursier.jar install scala:${SCALA_VERSION} scalac:${SCALA_VERSION} && \
    ln -s ~/.local/share/coursier/bin/scala /usr/local/bin/scala && \
    ln -s ~/.local/share/coursier/bin/scalac /usr/local/bin/scalac && \
    rm /tmp/coursier.jar

# Spark is excluded from the image to keep it lightweight (~350MB).
# Mount Spark into the container at runtime:
#   -v /path/to/spark:/opt/spark
# or set SPARK_HOME in devcontainer.json.

# protoc (architecture-aware download)
RUN arch=$(uname -m) && \
    case $arch in \
        x86_64) protoc_arch=x86_64 ;; \
        aarch64|arm64) protoc_arch=aarch_64 ;; \
        *) echo "unsupported arch: $arch"; exit 1 ;; \
    esac && \
    curl -fsSL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip \
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

# Copy pom.xml to enable Maven dependency caching (run mvn manually for full download)
COPY pom.xml .
# Note: dependencies are downloaded on first `mvn compile` inside the container
