# ailusion-journey-data-collector

Airflow thu thập dữ liệu du lịch (điểm đến, giá, thời tiết) đổ về kho dữ liệu
cho `ailusion-journey-be` dùng.

Repo hiện **chỉ có phần setup Airflow**, chưa có DAG hay job nào.

## Yêu cầu

- Docker Desktop đang chạy, cấp tối thiểu **4 GB RAM** và **2 CPU**
- Port `8081` còn trống (xem mục Port bên dưới)

## Chạy

```bash
cp .env.example .env
docker compose up -d --build
```

Lần đầu mất vài phút vì phải kéo image `apache/airflow:3.0.2` và build.

Kiểm tra đã lên chưa:

```bash
docker compose ps                      # tất cả service phải (healthy)
curl -s localhost:8081/api/v2/version  # apiserver trả về version
```

Mở web UI: <http://localhost:8081> — đăng nhập `airflow` / `airflow`
(đổi trong `.env`).

## Cấu trúc

```
docker-compose.yaml   Cụm Airflow: apiserver, scheduler, dag-processor,
                      worker, triggerer, init + postgres + redis
Dockerfile            Image dựa trên apache/airflow:3.0.2, cài requirements.txt
requirements.txt      Dependency cho DAG (giữ tối thiểu)
.env.example          Mẫu biến môi trường, copy thành .env
dags/                 Nơi đặt DAG (đang trống)
plugins/              Operator/hook tự viết
config/               airflow.cfg tự sinh khi chạy lần đầu (không commit)
tests/                Test cho DAG
logs/                 Log Airflow (không commit)
```

