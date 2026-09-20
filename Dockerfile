FROM apache/airflow:3.0.2

# Thêm thư mục airflow vào PYTHONPATH để import module trong repo
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
ENV PYTHONUNBUFFERED=1

# Image gốc đã chạy sẵn user airflow. Không đổi sang root vì requirements
# hiện tại đều có sẵn wheel, không phải biên dịch nên không cần compiler.
#
# Khi nào thêm dependency phải build từ source (psycopg2 bản không -binary,
# lxml, confluent-kafka...) thì chèn lại khối này trước lệnh pip:
#
#   USER root
#   RUN apt-get update && apt-get install -y gcc g++ \
#       && apt-get clean && rm -rf /var/lib/apt/lists/*
#   USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow
