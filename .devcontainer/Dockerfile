# https://github.com/microsoft/vscode-dev-containers/blob/main/containers/python-3/.devcontainer/Dockerfile

# [Choice] Python version (use -bullseye variants on local arm64/Apple Silicon): 3, 3.10, 3.9, 3.8, 3.7, 3.6, 3-bullseye, 3.10-bullseye, 3.9-bullseye, 3.8-bullseye, 3.7-bullseye, 3.6-bullseye, 3-buster, 3.10-buster, 3.9-buster, 3.8-buster, 3.7-buster, 3.6-buster

ARG VARIANT
FROM mcr.microsoft.com/vscode/devcontainers/python:${VARIANT}

ARG DATABRICKS_CLUSTER_ID
ARG DATABRICKS_HOST
ARG DATABRICKS_TOKEN

RUN sudo apt update \
    && sudo apt-get install -y default-jre \
    && pip install databricks-connect==13.0.1 \
    && pip install -U pip

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

ENV SPARK_HOME /usr/local/lib/python3.9/site-packages/pyspark
ENV DATABRICKS_CLUSTER_ID ${DATABRICKS_CLUSTER_ID}
ENV DATABRICKS_HOST ${DATABRICKS_HOST}
ENV DATABRICKS_TOKEN ${DATABRICKS_TOKEN}
