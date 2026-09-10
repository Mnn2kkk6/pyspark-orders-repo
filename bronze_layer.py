# bronze_layer.py
# ---------------------------------------------------------------------------
# BRONZE LAYER: nạp dữ liệu THÔ từ nguồn vào lakehouse
# mục tiêu giữ nguyên dữ liệu gốc (kể cả dữ liệu lỗi) để
# có thể debug về sau, và không bao giờ phải đọc lại file orders.csv một lần nữa.
#
# Thao tác tại Bronze: thêm cột metadata mô tả nguồn gốc
# (source_file, load_time). KHÔNG lọc, KHÔNG sửa dữ liệu, KHÔNG ép kiểu,
# KHÔNG thêm cột nào khác ngoài 2 cột metadata .
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

from common import write_single_csv

SOURCE_CSV = "orders.csv"
BRONZE_OUTPUT = "output/bronze/orders.csv"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("BronzeLayer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ============================================================
    # BƯỚC 1: Đọc orders.csv với TẤT CẢ cột giữ nguyên dạng string.
    # Ở Bronze, ta không dùng inferSchema/ép kiểu số, vì nếu ép kiểu
    # ngay từ đầu, các dòng lỗi (order_id rỗng, amount âm/không phải số...)
    # có thể bị Spark biến thành null hoặc gây lỗi đọc file, khiến
    # mấ dữ liệu gốc trước khi tìm ra lỗi .
    
    # ============================================================
    df_raw = (
        spark.read
        .option("header", True)
        .csv(SOURCE_CSV)
    )

    print("--- Schema Bronze (giữ nguyên string, chưa ép kiểu) ---")
    df_raw.printSchema()

    # ============================================================
    # BƯỚC 2: CHỈ thêm đúng 2 cột metadata  — không thêm cột nào khác ngoài source_file và load_time.
    # ============================================================
    df_bronze = (
        df_raw
        .withColumn("source_file", input_file_name())
        .withColumn("load_time", current_timestamp())
    )

    print("--- Dữ liệu Bronze (10 dòng đầu, đã có source_file + load_time) ---")
    df_bronze.show(10, truncate=False)

    print(f"--- Tổng số dòng nạp vào Bronze: {df_bronze.count()} ---")

    # ============================================================
    # BƯỚC 3: Lưu vào 1 file CSV giữ nguyên raw kể cả dòng lỗi .
    # ============================================================
    write_single_csv(df_bronze, BRONZE_OUTPUT)

    spark.stop()


if __name__ == "__main__":
    main()
