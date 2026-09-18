FROM apache/airflow:2.9.1-python3.12

# 1. Gunakan root untuk instalasi system dependency.
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Kembalikan user ke Airflow untuk instalasi Python dependency.
USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt

# 3. Install seluruh Python dependency project.
RUN pip install --no-cache-dir \
    --constraint "${HOME}/constraints.txt" \
    -r /requirements.txt