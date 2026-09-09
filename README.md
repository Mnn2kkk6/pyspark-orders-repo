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
