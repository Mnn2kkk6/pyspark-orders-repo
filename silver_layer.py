# silver_layer.py
# ---------------------------------------------------------------------------
# SILVER LAYER: đọc dữ liệu thô từ Bronze, sau đó làm sạch và ép kiểu .
# Đây là nơi áp dụng các quy tắc chất lượng dữ liệu (data quality rules):
#   - Bỏ record thiếu order_id (order_id rỗng/null)
#   - Chỉ giữ amount > 0 (loại bỏ đơn hàng lỗi/âm)
#   - Chuẩn hóa status về UPPERCASE (gộp success/SUCCESS/Success thành 1 giá trị)
#   - Cast amount sang kiểu số, order_date sang kiểu date
#
# LƯU Ý QUAN TRỌNG: Silver chỉ giữ lại đúng 6 cột nghiệp vụ gốc của đơn hàng
# (order_id, customer_id, province, amount, status, order_date) — không mang
# theo source_file/load_time của Bronze, vì đó là metadata phục vụ debug nạp
# dữ liệu, không phải dữ liệu nghiệp vụ, không cần thiết ở Silver/Gold.
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, to_date

from common import write_single_csv

BRONZE_INPUT = "output/bronze/orders.csv"
SILVER_OUTPUT = "output/silver/orders.csv"

# Đúng 6 cột nghiệp vụ 
BUSINESS_COLUMNS = ["order_id", "customer_id", "province", "amount", "status", "order_date"]


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SilverLayer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ============================================================
    # BƯỚC 1: Đọc dữ liệu Bronze (vẫn ở dạng string),
    # rồi chọn đúng 6 cột nghiệp vụ, bỏ source_file/load_time ngay từ đầu.
    # ============================================================
    df_bronze = (
        spark.read
        .option("header", True)
        .csv(BRONZE_INPUT)
        .select(*BUSINESS_COLUMNS)
    )

    print(f"--- Tổng số dòng đọc từ Bronze: {df_bronze.count()} ---")

    # ============================================================
    # BƯỚC 2: Bỏ record thiếu order_id.
    # Dữ liệu rỗng trong CSV có thể là NULL (Spark tự nhận) hoặc do chuỗi rỗng "",
    # nên kiểm tra cả hai trường hợp bằng isNotNull() và != "".
    # ============================================================
    df_clean = df_bronze.filter(
        col("order_id").isNotNull() & (trim(col("order_id")) != "")
    )
    print(f"--- Sau khi bỏ order_id rỗng: {df_clean.count()} dòng ---")

    # ============================================================
    # BƯỚC 3: Cast amount sang kiểu số (double), rồi chỉ giữ amount > 0.
    # Cast trước để so sánh số học đúng thay vì so sánh chuỗi .
    # ============================================================
    df_clean = df_clean.withColumn("amount", col("amount").cast("double"))
    df_clean = df_clean.filter(col("amount") > 0)
    print(f"--- Sau khi chỉ giữ amount > 0: {df_clean.count()} dòng ---")

    # ============================================================
    # BƯỚC 4: Chuẩn hóa status về UPPERCASE và bỏ khoảng trắng thừa.
    # ============================================================
    df_clean = df_clean.withColumn("status", upper(trim(col("status"))))

    # ============================================================
    # BƯỚC 5: Cast order_date sang kiểu DateType thật (format yyyy-MM-dd).
    # ============================================================
    df_clean = df_clean.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

    # Sắp lại đúng thứ tự 6 cột nghiệp vụ (tránh trường hợp thứ tự bị xáo trộn)
    df_silver = df_clean.select(*BUSINESS_COLUMNS)

    print("--- Schema Silver sau khi ép kiểu (đúng 6 cột nghiệp vụ) ---")
    df_silver.printSchema()

    print("--- Dữ liệu Silver (10 dòng đầu, đã sạch) ---")
    df_silver.show(10)

    # ============================================================
    # BƯỚC 6: Ghi ra đúng 1 file CSV, không thừa cột, không phải thư mục part-.
    # ============================================================
    write_single_csv(df_silver, SILVER_OUTPUT)

    spark.stop()


if __name__ == "__main__":
    main()
