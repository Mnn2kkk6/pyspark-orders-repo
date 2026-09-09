# spark_orders.py
# ---------------------------------------------------------------------------
# Bài thực hành PySpark: xử lý dữ liệu đơn hàng (orders)
# Flow: tạo/đọc dữ liệu -> xử lý bằng Spark (select/filter/groupBy + SQL) -> ghi kết quả
#
# Toàn bộ output chỉ ghi ra CSV (không dùng Parquet). Mỗi "bảng" kết quả được
# ghi ra ĐÚNG 1 file .csv riêng trong thư mục output/ (không phải thư mục con
# nhiều file part- như Spark mặc định) để dễ xem, dễ push lên git.
# ---------------------------------------------------------------------------

import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)

OUTPUT_DIR = "output"


# ============================================================
# HÀM TIỆN ÍCH: ghi 1 DataFrame ra ĐÚNG 1 file CSV có tên do mình đặt.
# Spark mặc định ghi CSV ra 1 THƯ MỤC chứa nhiều file part-xxxx.csv (do xử lý
# song song nhiều partition). Với dữ liệu nhỏ (bảng tổng hợp báo cáo), ta
# muốn có 1 file .csv sạch sẽ để dễ mở/push git, nên hàm này:
#   1. coalesce(1)  -> gom về 1 partition duy nhất trước khi ghi
#   2. ghi ra thư mục tạm
#   3. tìm file part-*.csv trong thư mục tạm, đổi tên/di chuyển thành
#      output/<final_name>.csv
#   4. xóa thư mục tạm
# ============================================================
def write_single_csv(df, final_path: str) -> None:
    tmp_dir = final_path + "__tmp"

    # Xóa thư mục tạm cũ nếu còn sót từ lần chạy trước
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    # Tìm file part-*.csv Spark vừa ghi ra trong thư mục tạm
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"Khong tim thay file part-*.csv trong {tmp_dir}")

    # Xóa file đích cũ nếu đã tồn tại, rồi move file part- vào đúng tên mong muốn
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_files[0], final_path)

    # Dọn thư mục tạm (bao gồm _SUCCESS, .crc... Spark sinh ra)
    shutil.rmtree(tmp_dir)

    print(f"Da ghi: {final_path}")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ============================================================
    # BƯỚC 0: Khởi tạo SparkSession
    # ============================================================
    spark = (
        SparkSession.builder
        .appName("OrdersPractice")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 70)
    print("PHẦN 1: TẠO DATAFRAME ĐƠN HÀNG TRỰC TIẾP TRONG CODE")
    print("=" * 70)

    # ============================================================
    # BƯỚC 1: Tạo DataFrame đơn hàng thủ công, gồm 5 cột:
    # order_id, customer_id, province, amount, status.
    # Schema tường minh (StructType) để kiểm soát đúng kiểu dữ liệu.
    # ============================================================
    order_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("status", StringType(), True),
    ])

    sample_data = [
        (1, "CUS001", "Hanoi", 500000, "SUCCESS"),
        (2, "CUS002", "HCM", 1200000, "PENDING"),
        (3, "CUS003", "Hanoi", 300000, "SUCCESS"),
        (4, "CUS004", "DaNang", 800000, "CANCELLED"),
        (5, "CUS005", "HCM", 2000000, "SUCCESS"),
        (6, "CUS006", "HaiPhong", 150000, "FAILED"),
        (7, "CUS007", "Hanoi", 950000, "SUCCESS"),
        (8, "CUS008", "CanTho", 400000, "SUCCESS"),
    ]

    df_orders = spark.createDataFrame(sample_data, schema=order_schema)

    # BƯỚC 2: printSchema()
    print("\n--- printSchema() ---")
    df_orders.printSchema()

    # BƯỚC 3: show()
    print("--- show() toàn bộ dữ liệu ---")
    df_orders.show()

    # BƯỚC 4: select() một số cột
    print("--- select('order_id', 'province', 'amount') ---")
    df_orders.select("order_id", "province", "amount").show()

    # BƯỚC 5: filter status = SUCCESS
    print("--- filter(status = 'SUCCESS') ---")
    df_success = df_orders.filter(col("status") == "SUCCESS")
    df_success.show()

    # BƯỚC 6: groupBy province -> count + sum(amount), chỉ tính đơn SUCCESS
    print("--- groupBy(province) -> count(order), sum(amount) [chỉ tính đơn SUCCESS] ---")
    df_group = (
        df_success.groupBy("province")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("amount").alias("total_amount"),
        )
        .orderBy(col("total_amount").desc())
    )
    df_group.show()

    # Ghi 3 "bảng" của Phần 1 ra CSV riêng, để đối chiếu với kết quả từ CSV thật ở Phần 2
    write_single_csv(df_orders, f"{OUTPUT_DIR}/1_orders_sample_raw.csv")
    write_single_csv(df_success, f"{OUTPUT_DIR}/2_orders_sample_success.csv")
    write_single_csv(df_group, f"{OUTPUT_DIR}/3_orders_sample_by_province.csv")

    print("\n" + "=" * 70)
    print("PHẦN 2: ĐỌC DỮ LIỆU TỪ FILE orders.csv (chạy create_orders_csv.py trước)")
    print("=" * 70)

    # ============================================================
    # BƯỚC 7: Đọc file orders.csv bằng PySpark
    # ============================================================
    df_csv = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("orders.csv")
    )

    print("--- Schema đọc từ CSV (inferSchema) ---")
    df_csv.printSchema()

    print("--- 10 dòng đầu của orders.csv ---")
    df_csv.show(10)

    # BƯỚC 8: Lặp lại select / filter / groupBy trên dữ liệu đọc từ CSV
    print("--- select một số cột từ CSV ---")
    df_csv.select("order_id", "province", "status", "amount").show(10)

    print("--- filter status = SUCCESS trên dữ liệu CSV ---")
    df_csv_success = df_csv.filter(col("status") == "SUCCESS")
    df_csv_success.show(10)

    print("--- groupBy(province) -> count(order), sum(amount) trên dữ liệu CSV ---")
    df_csv_group = (
        df_csv_success.groupBy("province")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("amount").alias("total_amount"),
        )
        .orderBy(col("total_amount").desc())
    )
    df_csv_group.show()

    # ============================================================
    # BƯỚC 9: Ghi các bảng kết quả của Phần 2 ra CSV (mỗi bảng 1 file riêng)
    # ============================================================
    write_single_csv(df_csv_success, f"{OUTPUT_DIR}/4_orders_full_success.csv")
    write_single_csv(df_csv_group, f"{OUTPUT_DIR}/5_orders_by_province.csv")

    print("\n" + "=" * 70)
    print("PHẦN 3: TEMP VIEW + SPARK SQL")
    print("=" * 70)

    # ============================================================
    # BƯỚC 10: Tạo temp view từ DataFrame để truy vấn bằng SQL thuần
    # ============================================================
    df_csv.createOrReplaceTempView("orders")

    # BƯỚC 11: SQL - tổng amount theo province (tất cả trạng thái)
    print("--- SQL: SUM(amount) theo province (tất cả trạng thái) ---")
    df_sql_all = spark.sql("""
        SELECT province,
               COUNT(*)     AS total_orders,
               SUM(amount)  AS total_amount
        FROM orders
        GROUP BY province
        ORDER BY total_amount DESC
    """)
    df_sql_all.show()

    # BƯỚC 12: SQL - tương đương filter SUCCESS + group by province
    print("--- SQL: SUM(amount) theo province, chỉ tính status = SUCCESS ---")
    df_sql_success = spark.sql("""
        SELECT province,
               COUNT(*)     AS total_orders,
               SUM(amount)  AS total_amount
        FROM orders
        WHERE status = 'SUCCESS'
        GROUP BY province
        ORDER BY total_amount DESC
    """)
    df_sql_success.show()

    # Ghi 2 bảng kết quả SQL ra CSV riêng
    write_single_csv(df_sql_all, f"{OUTPUT_DIR}/6_province_totals_all_status_sql.csv")
    write_single_csv(df_sql_success, f"{OUTPUT_DIR}/7_province_totals_success_sql.csv")

    # BƯỚC 13: Dừng SparkSession
    spark.stop()

    print("\nHoàn tất. Tất cả các bảng kết quả đã được ghi vào thư mục output/:")
    for f in sorted(glob.glob(f"{OUTPUT_DIR}/*.csv")):
        print(f"  - {f}")


if __name__ == "__main__":
    main()
