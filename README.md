# Bài thực hành PySpark: Xử lý dữ liệu đơn hàng 

Flow tổng quát: **dạng ETL**

## Cấu trúc project

```
pyspark-orders/
├── create_orders_csv.py     # sinh file orders.csv mẫu 
├── orders.csv                # dữ liệu đầu vào 
├── spark_orders.py           # script PySpark chính, làm toàn bộ yêu cầu bài thực hành
├── output/                   # mỗi bảng kết quả là 1 file .csv riêng 
│   ├── 1_orders_sample_raw.csv               # DataFrame mẫu 
│   ├── 2_orders_sample_success.csv           # đã filter status = SUCCESS (từ DataFrame mẫu)
│   ├── 3_orders_sample_by_province.csv       # group theo province (từ DataFrame mẫu)
│   ├── 4_orders_full_success.csv             # đã filter status = SUCCESS (từ orders.csv)
│   ├── 5_orders_by_province.csv              # group theo province (từ orders.csv) — bảng kết quả chính
│   ├── 6_province_totals_all_status_sql.csv  # kết quả Spark SQL, tổng tất cả trạng thái
│   └── 7_province_totals_success_sql.csv     # kết quả Spark SQL, chỉ tính SUCCESS
└── README.md
```

## Yêu cầu môi trường

- Python 3
- Java (JDK nào cũng đều được nhưng Spark cần JVM để chạy)
- PySpark 

## Các bước thực hiện

### Bước 1 — Tạo DataFrame đơn hàng bằng code (`spark_orders.py`)

- Định nghĩa explicit schema bằng `StructType`/`StructField` cho 5 cột:
  `order_id (int), customer_id (string), province (string), amount (int), status (string)`.
- Tạo DataFrame từ một list dữ liệu mẫu bằng `spark.createDataFrame(data, schema)`.
- Lý do định nghĩa explicit schema thay vì để Spark tự đoán giúp kiểm soát đúng kiểu dữ liệu ngay từ đầu.

### Bước 2 — Các thao tác cơ bản trên DataFrame

| Thao tác | Hàm dùng | Mục đích |
|---|---|---|
| Xem cấu trúc | `df.printSchema()` | in tên cột + kiểu dữ liệu + nullable |
| Xem dữ liệu | `df.show()` | in nội dung ra console |
| Chọn cột | `df.select("order_id", "province", "amount")` | chỉ lấy các cột cần |
| Lọc dữ liệu | `df.filter(col("status") == "SUCCESS")` | chỉ giữ đơn hàng thành công |
| Gom nhóm + tổng hợp | `df.groupBy("province").agg(count(...), sum(...))` | đếm số đơn & tổng tiền theo tỉnh |

Thứ tự áp dụng đúng theo đề: **filter SUCCESS trước rồi mới groupBy** để ra số liệu doanh thu/đơn hàng
thực tế theo từng tỉnh (chỉ tính đơn thành công).

### Bước 3 — Sinh file `orders.csv` và đọc bằng PySpark (Phần 2)

1. Chạy `python create_orders_csv.py` để sinh file `orders.csv` (200 dòng dữ liệu ngẫu nhiên,
   có cố định `random.seed` để kết quả tái lập được).
2. Trong `spark_orders.py`, đọc file bằng:
   ```python
   df_csv = spark.read.option("header", True).option("inferSchema", True).csv("orders.csv")
   ```
   - `header=True`: dòng đầu là tên cột.
   - `inferSchema=True`: Spark tự suy luận kiểu dữ liệu (int/string) từ dữ liệu thực tế.
3. Lặp lại đúng các thao tác select / filter / groupBy như Bước 2 nhưng trên dữ liệu đọc từ CSV.

### Bước 4 — Ghi kết quả ra CSV (mỗi bảng 1 file riêng)

Spark mặc định ghi CSV ra 1 **thư mục** chứa nhiều file `part-xxxx.csv` (do xử lý song song
nhiều partition), không tiện để mở/push git. Script dùng hàm `write_single_csv()` để
gom về đúng 1 file `.csv` sạch sẽ cho mỗi bảng:

```python
def write_single_csv(df, final_path: str) -> None:
    tmp_dir = final_path + "__tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)  # ghi tạm
    part_file = glob.glob(f"{tmp_dir}/part-*.csv")[0]   # tìm file part- Spark vừa ghi
    shutil.move(part_file, final_path)                   # đổi tên thành file đích
    shutil.rmtree(tmp_dir)                                # dọn thư mục tạm
```

Toàn bộ 7 bảng được tạo ra trong quá trình xử lý (Phần 1, Phần 2, Phần 3) đều được ghi ra CSV
theo cách này — xem danh sách đầy đủ ở mục "Cấu trúc project" phía trên.

### Bước 5 — Temp View + Spark SQL

```python
df_csv.createOrReplaceTempView("orders")   # đăng ký DataFrame như 1 bảng ảo để query bằng SQL

spark.sql("""
    SELECT province, COUNT(*) AS total_orders, SUM(amount) AS total_amount
    FROM orders
    WHERE status = 'SUCCESS'
    GROUP BY province
    ORDER BY total_amount DESC
""").show()
```

Temp view chỉ tồn tại trong phiên `SparkSession` hiện tại — mất đi khi job kết thúc.

## Cách chạy


**Cài đặt:**

1. Cài Java nếu chưa có — kiểm tra bằng `java -version`.
2. Cài Python 3.11 (tương thích với PySpark hơn Python 3.13 trên Windows).
3. Tạo virtual environment riêng bằng Python 3.11 trong thư mục project:
   ```powershell
   cd đường-dẫn-tới\pyspark-orders-repo
   py -3.11 -m venv venv311
   venv311\Scripts\activate
   pip install pyspark
   ```
4. Tải `winutils.exe` + `hadoop.dll` (Spark trên Windows cần để ghi file ra ổ đĩa):
   ```powershell
   mkdir C:\hadoop\bin -Force
   cd C:\hadoop\bin
   Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "winutils.exe"
   Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "hadoop.dll"
   ```
   (Lệnh tải báo lỗi SSL nên đã chạy `[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12` trước rồi mới thử lại.)

**Chạy lệnh** :

```powershell
cd đường-dẫn-tới\pyspark-orders-repo
venv311\Scripts\activate
$env:PYSPARK_PYTHON = ".\venv311\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = ".\venv311\Scripts\python.exe"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
python create_orders_csv.py
python spark_orders.py
```

Xem kết quả sau khi chạy:
```powershell
Get-Content output\5_orders_by_province.csv
```

**Đặt biến môi trường cố định** — tùy chọn:
Windows → gõ "environment variables" → **Edit the system environment variables** →
**Environment Variables...** → User variables → **New...** → thêm lần lượt:
- `PYSPARK_PYTHON` = đường dẫn đầy đủ tới `venv311\Scripts\python.exe`
- `PYSPARK_DRIVER_PYTHON` = đường dẫn đầy đủ tới `venv311\Scripts\python.exe`
- `HADOOP_HOME` = `C:\hadoop`

Sau đó thêm `C:\hadoop\bin` vào biến `Path` (User variables → chọn `Path` → **Edit...** → **New**).
Đóng và mở lại PowerShell mới để áp dụng. Khi đó chỉ cần `venv311\Scripts\activate` rồi chạy script,
không cần gõ lại các dòng `$env:...` nữa.

**Các lỗi thường gặp trên Windows và cách xử lý:**

| Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Python worker failed to connect back` / `SocketTimeoutException` | Đang chạy bằng Python không tương thích (chưa activate `venv311`, hoặc dính lại Python giả của Store) | Chạy lại đủ khối lệnh "Chạy hằng ngày" ở trên, kiểm tra đã thấy `(venv311)` ở đầu dòng lệnh |
| `WinError 10038` / Python worker crash khi `.show()` | Lỗi tương thích Python 3.13 với PySpark trên Windows | Dùng Python 3.11 trong venv riêng như hướng dẫn Bước 3 ở trên |
| `HADOOP_HOME and hadoop.home.dir are unset` khi ghi CSV/Parquet | Spark trên Windows cần `winutils.exe` để thao tác file | Cần tải winutils + set `HADOOP_HOME` |
| `python spark_orders.py` báo `[Errno 2] No such file or directory` | Đang đứng nhầm thư mục | Chạy `dir` để xem có `spark_orders.py` trong thư mục hiện tại không, `cd` thêm 1 cấp nếu cần |

## Kết quả mẫu (group theo province, chỉ tính status = SUCCESS)

| province | total_orders | total_amount |
|---|---|---|
| Hanoi | 30 | 77,602,455 |
| CanTho | 32 | 76,008,581 |
| DaNang | 24 | 54,721,530 |
| HCM | 23 | 53,386,225 |
| HaiPhong | 18 | 42,406,428 |

## 1 số kinh nghiệm tự rút ra được sau bài

- **DataFrame API vs Spark SQL**: hai cách viết khác nhau nhưng cùng optimizer (Catalyst) bên dưới,
  chọn cách nào tiện hơn cho từng tình huống — code phức tạp dùng DataFrame API dễ compose,
  báo cáo/truy vấn nhanh dùng SQL trực quan hơn.
- **inferSchema vs Explicit schema**: đọc CSV nhỏ/thử nghiệm dùng `inferSchema` cho nhanh,
  nhưng pipeline thực tế nên khai schema tường minh để tránh Spark đoán sai kiểu và tránh
  phải quét dữ liệu 2 lần (1 lần đoán schema, 1 lần đọc thật).
- **coalesce(1) khi ghi CSV**: Spark mặc định ghi song song ra nhiều file `part-*` theo số partition;
  gom về 1 file chỉ nên làm với dữ liệu nhỏ (như file tổng hợp group by), không nên làm với dữ liệu lớn
  vì mất khả năng ghi song song.


---

# Phần 2 : Data Lakehouse Bronze/Silver/Gold + MinIO

Dùng lại đúng các thao tác PySpark ở Phần 1 (printSchema, show, select, filter,
groupBy) nhưng đặt vào flow kiến trúc **Medallion Architecture** (Bronze/Silver/Gold),
và lưu output lên **MinIO** .

```
orders.csv (có lỗi cố ý)
        │  Spark đọc
        ▼
┌─────────────┐   thêm metadata      ┌─────────────┐   clean + ép kiểu     ┌─────────────┐   group by
│   BRONZE    │  (source_file,       │   SILVER    │  (bỏ record lỗi,      │    GOLD     │  province,
│  (raw data) │───load_time)────────▶│ (clean data)│───chuẩn hóa status)──▶│  (summary)  │  tính 4 chỉ số
└─────────────┘                      └─────────────┘                       └─────────────┘
        │                                    │                                    │
        └────────────────────────────────────┴────────────────────────────────────┘
                                              │ upload
                                              ▼
                                    MinIO bucket "lakehouse-demo"
                                    bronze/orders, silver/orders, gold/order_summary
```

## File mới thêm vào repo (Phần 2)

```
pyspark-orders-repo/
├── common.py                  # hàm write_single_csv() — TÁI SỬ DỤNG từ spark_orders.py Phần 1
├── create_orders_dirty.py     # sinh orders.csv MỚI (250 dòng, có 30 dòng lỗi cố ý, thêm cột order_date)
├── bronze_layer.py            # đọc orders.csv -> thêm source_file/load_time -> ghi 1 file CSV
├── silver_layer.py            # đọc Bronze -> clean + ép kiểu -> ghi 1 file CSV (đúng 6 cột nghiệp vụ)
├── gold_layer.py               # đọc Silver -> group by province -> ghi 1 file CSV (đúng 5 cột)
├── upload_to_minio.py         # tạo bucket + upload 3 file Bronze/Silver/Gold lên MinIO
├── run_lakehouse_pipeline.py  # chạy cả 5 bước trên chỉ bằng 1 lệnh
├── docker-compose.yml          # chạy MinIO local để thực hành
├── requirements.txt            # pyspark + minio
└── output/
    ├── bronze/orders.csv           # 8 cột: 6 cột gốc + source_file + load_time, GIỮ NGUYÊN dòng lỗi
    ├── silver/orders.csv           # đúng 6 cột nghiệp vụ, đã lọc sạch + ép kiểu
    └── gold/order_summary.csv      # đúng 5 cột: province + 4 chỉ số tổng hợp
```

Lưu ý: `create_orders_dirty.py` sinh **file `orders.csv` mới** (6 cột, có thêm
`order_date` và dữ liệu lỗi cố ý) — sẽ **ghi đè** `orders.csv` cũ của Phần 1
(vốn chỉ có 5 cột, không có dữ liệu lỗi). Nếu muốn giữ cả 2 bộ dữ liệu, đổi tên
`orders.csv` cũ trước khi chạy `create_orders_dirty.py`, hoặc chấp nhận dùng
chung 1 file `orders.csv` (Phần 1 không phụ thuộc vào orders.csv có sẵn — script
`spark_orders.py` của Phần 1 sẽ báo lỗi nếu cột không khớp, nên chạy lại
`create_orders_csv.py` gốc của Phần 1 trước khi chạy lại `spark_orders.py`).

## Dữ liệu nguồn: `orders.csv` (Phần 2)

6 cột: `order_id, customer_id, province, amount, status, order_date`

| Loại lỗi cố ý | Số dòng | Ví dụ |
|---|---|---|
| `order_id` rỗng | 15 | `,CUS023,HCM,4989656,success,2026-01-05` |
| `amount` ≤ 0 | 15 | `231,CUS033,CanTho,-1,SUCCESS,2026-07-23` |
| `status` viết hoa/thường lẫn lộn | rải khắp | `success`, `SUCCESS`, `Success`, `failed`, `FAILED`, `Failed`... |

Tổng cộng 250 dòng (220 dòng sạch + 30 dòng lỗi), seed cố định để tái lập được.

## Nguyên tắc quan trọng: KHÔNG cột thừa qua từng layer

| Layer | Số cột | Danh sách cột |
|---|---|---|
| Bronze | 8 | 6 cột gốc + `source_file` + `load_time` ( thêm 2 cột metadata) |
| Silver | 6 | Đúng 6 cột nghiệp vụ gốc — **bỏ hẳn** `source_file`/`load_time` |
| Gold | 5 | `province` + `total_orders` + `total_amount` + `success_orders` + `failed_orders`  |


## Chi tiết từng layer

### Bronze — `bronze_layer.py`

- Đọc `orders.csv` **giữ nguyên toàn bộ dạng string**, không ép kiểu hay lọc bỏ .
- Thêm đúng 2 cột: `source_file` (từ `input_file_name()`), `load_time` (từ `current_timestamp()`).
- Ghi ra **1 file CSV duy nhất** `output/bronze/orders.csv` bằng `write_single_csv()`
  (tái dùng từ Phần 1) — giữ nguyên cả dòng lỗi, đúng nguyên tắc Bronze.

### Silver — `silver_layer.py`

Đọc Bronze, `.select()` ngay 6 cột nghiệp vụ (loại bỏ metadata), rồi làm theo thứ tự:

1. Bỏ record thiếu `order_id` (rỗng hoặc null)
2. Cast `amount` sang `double`, chỉ giữ `amount > 0`
3. Chuẩn hóa `status` về UPPERCASE
4. Cast `order_date` sang kiểu `date` thật (`to_date()`, format `yyyy-MM-dd`)

Kết quả: 250 dòng Bronze → 220 dòng Silver (loại 15 dòng thiếu order_id + 15 dòng
amount không hợp lệ). Lưu vào `output/silver/orders.csv`.

### Gold — `gold_layer.py`

Group theo `province`, tính đúng 4 chỉ số:

| Cột | Cách tính |
|---|---|
| `total_orders` | `count(order_id)` |
| `total_amount` | `sum(amount)` (cast về số nguyên, tránh hiển thị dạng khoa học) |
| `success_orders` | `sum(when(status == 'SUCCESS', 1).otherwise(0))` |
| `failed_orders` | `sum(when(status == 'FAILED', 1).otherwise(0))` |

Lưu vào `output/gold/order_summary.csv`.

## MinIO

### Chạy MinIO local

```bash
docker compose up -d
```

Web console: **http://localhost:9001** (đăng nhập bằng tài khoản: `minioadmin` / mật khẩu: `minioadmin123`).

### `upload_to_minio.py`

1. Kết nối MinIO qua thư viện `minio` (Python SDK chính thức).
2. Kiểm tra bucket `lakehouse-demo` đã tồn tại chưa (`bucket_exists`), chưa có thì tạo.
3. Upload đúng 3 file (mỗi layer 1 file) lên đúng path:

```
lakehouse-demo/
├── bronze/orders.csv
├── silver/orders.csv
└── gold/order_summary.csv
```

Nếu đã có MinIO chạy sẵn ở nơi khác thì phải set biến môi trường trước khi chạy script:
```bash
export MINIO_ENDPOINT="your-minio-host:9000"
export MINIO_ACCESS_KEY="your-access-key"
export MINIO_SECRET_KEY="your-secret-key"
export MINIO_SECURE="true"   # nếu dùng https
```

## Cách chạy (Phần 2)

### Windows (PowerShell)

Dùng lại đúng setup đã xử lý ở Phần 1 (Python 3.11 qua `venv311`, `HADOOP_HOME` trỏ
`winutils.exe`). Cài thêm Docker Desktop nếu chưa có: https://www.docker.com/products/docker-desktop/

```powershell
cd đường-dẫn-tới\pyspark-orders-repo
venv311\Scripts\activate
$env:PYSPARK_PYTHON = ".\venv311\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = ".\venv311\Scripts\python.exe"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"

pip install -r requirements.txt
docker compose up -d
python run_lakehouse_pipeline.py
```

Xem kết quả: `Get-Content output\gold\order_summary.csv`

### Dừng và dọn dẹp MinIO

```bash
docker compose down          # dừng container, giữ lại dữ liệu đã upload
docker compose down -v       # dừng container VÀ xóa luôn dữ liệu (volume)
```

## Một số kinh nghiệm rút ra sau bài

- Tách 3 layer để giữ lại dữ liệu gốc (Bronze), phòng khi cần xem lại hoặc xử lý lại từ đầu.
- Bronze giữ nguyên dữ liệu dạng chữ, chưa ép kiểu số/ngày, để tránh làm mất hoặc sai lệch dữ liệu lỗi ngay từ đầu.
- Silver và Gold không giữ cột `source_file`, `load_time` vì đó chỉ là thông tin phục vụ debug, không phải dữ liệu nghiệp vụ.
- MinIO dùng để giả lập kho lưu trữ đám mây (như S3), giúp luyện tập upload/tổ chức dữ liệu mà không cần tài khoản cloud thật.


---

# Phần 3: Airflow điều phối + khái niệm Iceberg/Nessie/Data Catalog

Nối tiếp Phần 2 (Bronze/Silver/Gold + MinIO), phần này thêm **Airflow** vào flow
để hiểu vai trò của một **orchestrator** (công cụ điều phối), và giới thiệu khái
niệm về Iceberg/Nessie/Data Catalog — 3 mảnh ghép để dữ liệu trên
lake trở thành "table" có version, có schema tra cứu được.

```
source CSV → Spark xử lý Bronze/Silver/Gold → MinIO lưu dữ liệu
                        ▲
                        │  điều phối (chạy đúng thứ tự, đúng lịch)
                    Airflow
                        │ 
                Iceberg/Nessie/Data Catalog
              quản lý table/metadata trên lake
```

## File mới thêm vào repo (Phần 3)

```
pyspark-orders-repo/
└── dags/
    └── bronze_silver_gold_dag.py   # DAG Airflow: 3 task bronze -> silver -> gold
```

`docker-compose.yml` được cập nhật thêm service `airflow` ,chạy Airflow ở chế độ
`standalone` — 1 container duy nhất gồm cả webserver, scheduler, DB sqlite;
 **không dùng cấu hình này cho production**.

## DAG: `bronze_silver_gold_dag.py`

- 3 task: `bronze_task`, `silver_task`, `gold_task`, mỗi task dùng `PythonOperator`
  gọi 1 hàm chỉ `print()` ra bước đang chạy — **CHƯA gọi thật** `bronze_layer.py` /
  `silver_layer.py` / `gold_layer.py` của Phần 2.
- Dependency: `bronze_task >> silver_task >> gold_task` — silver chỉ chạy sau khi
  bronze **thành công**, gold chờ silver, đúng thứ tự flow Lakehouse thực tế.
- `schedule=None`: DAG không tự chạy theo lịch, chỉ trigger thủ công trên UI .

Đã test bằng Airflow CLI thật (`airflow tasks test ... bronze_task ...`) — cả 3
task chạy đúng, in đúng nội dung `Running Bronze layer` / `Running Silver layer` /
`Running Gold layer`, Airflow tự đánh dấu SUCCESS.

## Cách chạy Airflow UI

```bash
docker compose up -d
```

Chờ để Airflow khởi tạo xong, rồi:

1. Mở **http://localhost:8080**
2. Đăng nhập user `admin`, lấy password bằng 1 trong 2 cách:
   ```bash
   docker compose logs airflow | grep "Password for user"
   ```
   hoặc đọc trực tiếp file password Airflow tự sinh ra trong container:
   ```bash
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```
3. Trên UI, tìm DAG tên **`bronze_silver_gold_dag`** trong danh sách.
4. Bật DAG (gạt toggle ở đầu dòng từ tắt sang bật).
5. Bấm nút ▶ (Trigger DAG) để chạy thử.
6. Vào Graph View, thấy đúng thứ tự 3 ô vuông nối tiếp: `bronze_task → silver_task → gold_task`.
7. Bấm vào từng task → tab **Logs** → xem đúng dòng `Running Bronze layer` /
   `Running Silver layer` / `Running Gold layer` đã in ra.

### Windows (PowerShell)

```powershell
cd đường-dẫn-tới\pyspark-orders-repo
docker compose up -d
docker compose logs airflow | Select-String "Password for user"
```
Mở trình duyệt http://localhost:8080, đăng nhập, làm các bước 3–7 ở trên.

## Ghi chú: vai trò của Airflow trong flow

> **Airflow không xử lý dữ liệu trực tiếp như Spark.** Spark là nơi dữ liệu THỰC SỰ
> được đọc/biến đổi/ghi (filter, groupBy, cast kiểu...). Airflow chỉ đóng vai trò
> **điều phối (orchestrate)** — quyết định: bước nào chạy trước, bước nào chạy sau,
> chạy lúc mấy giờ (schedule), nếu 1 bước lỗi thì có retry không, và cho phép xem
> lại log/trạng thái từng lần chạy trên 1 giao diện tập trung.


## Khái niệm: Iceberg / Nessie / Data Catalog


| Thành phần | Vai trò |
|---|---|
| **Spark** | Công cụ xử lý dữ liệu — đọc, biến đổi (filter/groupBy/join...), ghi dữ liệu. |
| **MinIO** | Nơi LƯU FILE vật lý (S3-compatible object storage) — chỉ biết "có file gì trong bucket", không biết file đó là 1 "bảng dữ liệu" có schema/lịch sử thay đổi. |
| **Iceberg** | Table format — biến 1 tập hợp file rời rạc trên MinIO thành 1 **"table"** có schema rõ ràng, hỗ trợ time travel (xem lại dữ liệu ở phiên bản cũ), cập nhật/xóa dòng dữ liệu (điều mà file CSV/Parquet thô không tự làm được). |
| **Nessie** | Catalog quản lý **version** của các Iceberg table — giống git nhưng cho dữ liệu: có thể tạo branch, commit thay đổi, rollback về version cũ của cả 1 tập hợp table. |
| **Data Catalog** | Nơi trả lời câu hỏi "hệ thống đang có những dataset/table nào, schema ra sao, nằm ở đâu" — giúp người dùng/công cụ khác (BI tool, data scientist) tìm và hiểu dữ liệu mà không cần hỏi trực tiếp đội kỹ thuật. |

**Cách các mảnh khớp lại với nhau** :
Thay vì Gold layer ghi CSV thô ra MinIO như hiện tại, ta sẽ ghi qua **Iceberg** —
lúc đó `gold/order_summary` không còn là "1 file CSV" nữa mà là 1 table thật sự
. **Nessie** đứng ra quản lý version của table đó. Và toàn bộ danh sách table + schema của chúng được đăng ký vào 
**Data Catalog** để các công cụ khác tra cứu được — đây chính là điểm khác biệt giữa
"data lake" (chỉ có file) và "**lakehouse**" (có file + có tính chất của table
trong database).
