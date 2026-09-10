# gold_layer.py
# ---------------------------------------------------------------------------
# GOLD LAYER: đọc dữ liệu sạch từ Silver, tổng hợp thành báo cáo
# business-ready — đây là dữ liệu dùng trực tiếp cho dashboard/báo cáo,
# không cần xử lý gì thêm.
#
# Tính theo từng province, đúng 4 chỉ số (không thêm cột nào khác ngoài province + 4 chỉ số này):
#   - total_orders: tổng số đơn hàng 
#   - total_amount: tổng giá trị đơn hàng 
#   - success_orders: số đơn có status = SUCCESS
#   - failed_orders: số đơn có status = FAILED
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when
from pyspark.sql.types import LongType

from common import write_single_csv

SILVER_INPUT = "output/silver/orders.csv"
GOLD_OUTPUT = "output/gold/order_summary.csv"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("GoldLayer")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ============================================================
    # BƯỚC 1: Đọc dữ liệu Silver .
    # inferSchema=True an toàn ở đây vì Silver đã được ép kiểu và ghi ra CSV
    # đúng chuẩn rồi (không còn dữ liệu lỗi như lúc đọc Bronze).
    # ============================================================
    df_silver = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(SILVER_INPUT)
    )

    print("--- Schema đọc từ Silver ---")
    df_silver.printSchema()
    print(f"--- Tổng số dòng đọc từ Silver: {df_silver.count()} ---")

    # ============================================================
    # BƯỚC 2: Group theo province, tính ĐÚNG 4 chỉ số yêu cầu.
    # - total_orders / total_amount: count(*)/sum(amount) trên toàn bộ dòng
    #   của tỉnh đó (không lọc status).
    # - success_orders / failed_orders
    #   sum(when(điều_kiện, 1).otherwise(0)) — tương đương
    #   COUNT(CASE WHEN ... THEN 1 END) trong SQL.
    # ============================================================
    df_gold = (
        df_silver.groupBy("province")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("amount").cast(LongType()).alias("total_amount"),
            spark_sum(when(col("status") == "SUCCESS", 1).otherwise(0)).alias("success_orders"),
            spark_sum(when(col("status") == "FAILED", 1).otherwise(0)).alias("failed_orders"),
        )
        .orderBy(col("total_amount").desc())
    )

    print("--- Gold: order_summary theo province (đúng 5 cột: province + 4 chỉ số) ---")
    df_gold.show()

    # ============================================================
    # BƯỚC 3: Lưu vào 1 file CSV, không thừa cột.
    # ============================================================
    write_single_csv(df_gold, GOLD_OUTPUT)

    spark.stop()


if __name__ == "__main__":
    main()
