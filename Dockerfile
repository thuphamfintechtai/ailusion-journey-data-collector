FROM apache/airflow:3.0.2

# Chuyển sang root để cài system package
USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thêm thư mục airflow vào PYTHONPATH để import module trong repo
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
ENV PYTHONUNBUFFERED=1

USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow
