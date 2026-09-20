# ailusion-journey-data-collector

Airflow thu thập dữ liệu du lịch (điểm đến, giá, thời tiết) đổ về kho dữ liệu
cho `ailusion-journey-be` dùng.

Repo hiện **chỉ có phần setup Airflow**, chưa có DAG hay job nào.

Có hai cách chạy:

| Cách | Khi nào dùng | Port |
|---|---|---|
| [Docker compose](#chạy-bằng-docker-compose) | Giống môi trường thật (Celery + Postgres) | 8081 |
| [Local](#chạy-airflow-local-không-docker) | Chỉ cần viết và parse DAG, nhẹ hơn | 8082 |

## Yêu cầu

- Docker Desktop đang chạy, cấp tối thiểu **4 GB RAM** và **2 CPU**
- Port `8081` còn trống (xem mục Port bên dưới)
- Bản local cần thêm `python3.12`

## Chạy bằng docker compose

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

Mở web UI: <http://localhost:8081> — đăng nhập `admin` / `admin`
(đổi trong `.env`, khoá `_AIRFLOW_WWW_USER_USERNAME` và
`_AIRFLOW_WWW_USER_PASSWORD`).

Tài khoản chỉ được tạo ở lần `up` đầu tiên, khi metadata database còn trống.
Đổi trong `.env` sau đó sẽ không có tác dụng — phải tạo lại bằng tay:

```bash
docker compose exec airflow-apiserver airflow users create \
  --username admin --password admin --firstname Admin --lastname User \
  --role Admin --email admin@ailusion.local
```

Hoặc xoá sạch database rồi dựng lại: `docker compose down -v && docker compose up -d`.

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

## Chạy Airflow local (không Docker)

Nhẹ hơn Docker, hợp khi chỉ cần viết và parse DAG. Dùng SQLite +
LocalExecutor nên **không phản ánh đúng môi trường thật** (thật là Celery +
Postgres) — trước khi merge vẫn nên chạy lại bằng `docker compose`.

### 1. Tạo venv bằng Python 3.12

Airflow 3.0.2 chưa hỗ trợ Python 3.13/3.14. Máy có `python3` là 3.14 nên
phải gọi đích danh `python3.12`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

Phải `source` để `airflow` nằm trong `PATH`. Gọi thẳng `./.venv/bin/airflow`
sẽ hỏng: `airflow standalone` spawn tiến trình con bằng cách gọi lại lệnh
`airflow` từ `PATH`, không thấy thì chết với
`FileNotFoundError: [Errno 2] No such file or directory: 'airflow'`.

### 2. Cài Airflow

Bắt buộc dùng file constraint, nếu không pip sẽ resolve ra tổ hợp version hỏng:

```bash
pip install "apache-airflow==3.0.2" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
```

### 3. Chạy

```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT=8082

airflow standalone
```

`AIRFLOW_HOME` bắt buộc phải trỏ vào repo. Mặc định Airflow dùng `~/airflow`
— dùng chung với mọi project khác trên máy, dễ lẫn metadata database.

`airflow standalone` **không có cờ `--port`**, đổi port bằng
`AIRFLOW__API__PORT` như trên. Dùng 8082 để không đụng 8080
(finance-intelligence-hub) và 8081 (bản Docker của repo này).

Mở <http://localhost:8082>, đăng nhập `admin` / `admin`.

Bản local dùng SimpleAuthManager, nó **tự sinh mật khẩu ngẫu nhiên** ở lần chạy
đầu và ghi vào file dưới đây. Muốn dùng `admin`/`admin` cho khớp bản Docker thì
sửa thẳng file rồi khởi động lại:

```bash
echo '{"admin": "admin"}' > .airflow/simple_auth_manager_passwords.json.generated
```

```
.airflow/simple_auth_manager_passwords.json.generated
```

Kiểm tra các thành phần đã chạy:

```bash
airflow db check                                 # Connection successful
airflow jobs check --job-type SchedulerJob       # Found one alive job
airflow jobs check --job-type TriggererJob       # Found one alive job
airflow dags list                                # No data found (chưa có DAG)
```

Dừng: `Ctrl-C`. Xoá sạch để làm lại: `rm -rf .airflow`.

### Lỗi SIGSEGV trên macOS

Trên macOS, log của `airflow standalone` sẽ đầy dòng:

```
scheduler  | [ERROR] Worker (pid:59620) was sent SIGSEGV!
triggerer  | [ERROR] Worker (pid:59622) was sent SIGSEGV!
```

Đây là tiến trình **log-server** (gunicorn) mà scheduler và triggerer spawn
kèm, bị lỗi fork-safety của macOS. Đã kiểm: **không ảnh hưởng chức năng** —
scheduler và triggerer vẫn báo "Found one alive job", web UI và REST API vẫn
trả 200, tổng CPU của các tiến trình Airflow chỉ ~2.6%.

Giảm bớt (không hết hẳn):

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy='*'
```

Muốn log sạch hẳn thì dùng bản Docker — container chạy Linux nên không dính
lỗi này.

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

## Quy ước code

Thống nhất với `ailusion-journey-mcp`:

- **Không dùng icon / emoji** ở bất cứ đâu — log, `print()`, docstring, README
- **Log message viết bằng tiếng Việt**, dạng câu ngắn mô tả việc đang xảy ra
- **Ghi chú (comment, docstring) viết bằng tiếng Việt**, giải thích *vì sao*
  chứ không lặp lại *cái gì* mà code đã nói
- Tên biến, tên hàm, tên DAG, `task_id`: giữ không dấu để grep được

## Repo liên quan

| Repo | Vai trò |
|---|---|
| [`ailusion-journey-fe`](https://github.com/thuphamfintechtai/ailusion-journey-fe) | Next.js — giao diện người dùng |
| [`ailusion-journey-be`](https://github.com/thuphamfintechtai/ailusion-journey-be) | Node.js — API, dữ liệu, nghiệp vụ |
| [`ailusion-journey-llm`](https://github.com/thuphamfintechtai/ailusion-journey-llm) | Python — suy luận LLM |
| [`ailusion-journey-mcp`](https://github.com/thuphamfintechtai/ailusion-journey-mcp) | Python — lớp MCP cho LLM gọi tool |
| `ailusion-journey-data-collector` | **repo này** — Airflow thu thập dữ liệu |

Luồng dữ liệu:

```
data-collector (Airflow)  ->  kho dữ liệu  ->  be (API)  ->  mcp (tool)  ->  llm / client
```
