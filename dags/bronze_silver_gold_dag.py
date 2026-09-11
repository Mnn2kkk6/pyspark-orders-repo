# bronze_silver_gold_dag.py
# ---------------------------------------------------------------------------
# DAG Airflow đơn giản mô phỏng flow Bronze -> Silver -> Gold đã làm bằng
# PySpark ở bài trước. Ở bài này, Airflow chưa gọi thật các script
# bronze_layer.py/silver_layer.py/gold_layer.py — mỗi task chỉ in ra bước
# đang chạy, để tập trung hiểu đúng khái niệm cốt lõi: Airflow điều phối
# thứ tự chạy các bước, KHÔNG PHẢI công cụ xử lý dữ liệu.
#
# (Hướng phát triển tiếp theo, không nằm trong bài này: đổi PythonOperator
# thành BashOperator gọi thẳng "python bronze_layer.py" để Airflow điều phối
# job Spark thật — nhưng cần cài pyspark/Java bên trong môi trường Airflow
# và cấu hình kết nối MinIO, phức tạp hơn nên chưa làm ở bước này.)
# ---------------------------------------------------------------------------

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# 3 hàm Python tương ứng 3 bước trong flow Lakehouse.
# Chỉ cần print ra bước đang chạy, chưa gọi Spark thật.
# ============================================================
def run_bronze():
    print("Running Bronze layer")


def run_silver():
    print("Running Silver layer")


def run_gold():
    print("Running Gold layer")


# ============================================================
# Cấu hình DAG: chạy thủ công (schedule=None), không catchup dữ liệu cũ.
# ============================================================
with DAG(
    dag_id="bronze_silver_gold_dag",
    description="DAG demo mô phỏng flow Bronze -> Silver -> Gold (chỉ print, chưa gọi Spark thật)",
    start_date=datetime(2026, 1, 1),
    schedule=None,   # không tự động chạy theo lịch, chỉ trigger thủ công trên UI
    catchup=False,   # không chạy bù các lần lẽ ra đã chạy trong quá khứ
    tags=["lakehouse", "demo"],
) as dag:

    # ============================================================
    # BƯỚC 1: Định nghĩa 3 task, mỗi task gọi 1 hàm Python tương ứng.
    # PythonOperator: cách đơn giản nhất để chạy 1 đoạn code Python như 1 task.
    # ============================================================
    bronze_task = PythonOperator(
        task_id="bronze_task",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_task",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_task",
        python_callable=run_gold,
    )

    # ============================================================
    # BƯỚC 2: Set dependency — thứ tự chạy bắt buộc: bronze -> silver -> gold.
    # Toán tử >> nghĩa là "chạy trước, rồi mới đến": silver_task chỉ bắt đầu
    # sau khi bronze_task chạy thành công, tương tự gold_task chờ silver_task.
    # Đây chính là cách Airflow đảm bảo đúng thứ tự flow Lakehouse thực tế
    # (không thể Silver clean dữ liệu khi Bronze chưa nạp xong).
    # ============================================================
    bronze_task >> silver_task >> gold_task
