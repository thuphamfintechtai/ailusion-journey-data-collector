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

### Port

Web UI mặc định là `8080`, nhưng `.env.example` đặt `AIRFLOW_PORT=8081`.
Lý do: `finance-intelligence-hub` cũng là một cụm Airflow, đang giữ `8080`
và khai báo `restart: always` nên tự bật lại mỗi lần Docker Desktop khởi động.
Hai cụm dùng chung port sẽ không bind được.

Muốn đổi thì sửa `AIRFLOW_PORT` (và `FLOWER_PORT`) trong `.env`.
Kiểm tra ai đang giữ port:

```bash
docker ps --format '{{.Names}}\t{{.Ports}}' | grep 8080
```

Dừng:

```bash
docker compose down          # dừng, giữ lại database
docker compose down -v       # dừng và xoá sạch database Airflow
```

## Test nhanh khi chưa có DAG

Repo chưa có DAG nên danh sách trong UI sẽ trống. Muốn có DAG để bấm thử,
bật DAG mẫu của Airflow:

```bash
# sửa .env
AIRFLOW__CORE__LOAD_EXAMPLES=true

docker compose up -d
```

Vào UI sẽ thấy các DAG `example_*`, bật một cái lên và trigger để xác nhận
scheduler + worker chạy đúng. Xong thì đặt lại `false`.

Chạy lệnh Airflow CLI trong container:

```bash
docker compose run --rm airflow-cli airflow version
docker compose run --rm airflow-cli airflow dags list
```

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

## Kiến trúc

CeleryExecutor — scheduler đẩy task qua Redis, worker nhận và chạy.
Metadata nằm ở PostgreSQL riêng trong compose.

| Service | Vai trò | Port |
|---|---|---|
| `airflow-apiserver` | Web UI + REST API | 8081 (host) |
| `airflow-scheduler` | Lên lịch task | |
| `airflow-dag-processor` | Parse file trong `dags/` | |
| `airflow-worker` | Chạy task (Celery) | |
| `airflow-triggerer` | Chạy deferrable operator | |
| `postgres` | Metadata database | |
| `redis` | Celery broker | |
| `flower` | Theo dõi Celery, bật bằng `--profile flower` | 5556 (host) |

## Thêm DAG

Đặt file `.py` vào `dags/`. Thư mục được mount thẳng vào container nên
dag-processor tự nhận sau vài chục giây, không cần restart.

