# Use Python 3.10 slim version based on Debian Bullseye as the base image
ARG IMAGE_PYTHON_VERSION=3.10
ARG BASE_PYTHON_TAG="${IMAGE_PYTHON_VERSION}-slim-bullseye"

FROM python:${BASE_PYTHON_TAG} AS base

LABEL maintainer="Shimasadat Hosseini"

# Set environment variables
ENV OPENJDK_VERSION="11" \
    SPARK_VERSION="3.3.2" \
    HADOOP_VERSION="3" \
    SPARK_HOME="/opt/spark" \
    PYSPARK_PYTHON="/usr/bin/python${IMAGE_PYTHON_VERSION}" \
    PYSPARK_DRIVER_PYTHON="/usr/bin/python${IMAGE_PYTHON_VERSION}"

# Install dependencies and OpenJDK
RUN apt-get update -y && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    openjdk-${OPENJDK_VERSION}-jre-headless \
    ca-certificates-java \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add Spark to PATH
ENV PATH="${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin"

# Install Python dependencies
COPY requirements.txt /workspace/requirements.txt
RUN pip install --upgrade pip && \
    pip install -r /workspace/requirements.txt

# Create a working directory
WORKDIR /workspace

# Copy the project files into the container
COPY . /workspace

# Set up entrypoint for the container
CMD ["bash"]



