# run_lakehouse_pipeline.py
# ---------------------------------------------------------------------------
# Chạy toàn bộ flow Lakehouse chỉ với 1 lệnh: sinh dữ liệu -> Bronze -> Silver
# -> Gold -> upload MinIO. Đặt tên file "run_lakehouse_pipeline.py" để không
# đụng tới spark_orders.py ( giữ nguyên, chạy độc lập).
#
# Chạy từng bước riêng để debug:
#   python create_orders_dirty.py
#   python bronze_layer.py
#   python silver_layer.py
#   python gold_layer.py
#   python upload_to_minio.py
# ---------------------------------------------------------------------------

import subprocess
import sys

STEPS = [
    ("Sinh dữ liệu orders.csv (có lỗi cố ý)", "create_orders_dirty.py"),
    ("Bronze layer", "bronze_layer.py"),
    ("Silver layer", "silver_layer.py"),
    ("Gold layer", "gold_layer.py"),
    ("Upload lên MinIO", "upload_to_minio.py"),
]


def run_step(title: str, script: str) -> None:
    print("\n" + "=" * 70)
    print(f"BƯỚC: {title}  ({script})")
    print("=" * 70)
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"\n[DỪNG] Bước '{title}' thất bại (script {script} trả mã lỗi {result.returncode}).")
        sys.exit(result.returncode)


def main() -> None:
    for title, script in STEPS:
        run_step(title, script)

    print("\n" + "=" * 70)
    print("HOÀN TẤT PIPELINE: source CSV -> Bronze -> Silver -> Gold -> MinIO")
    print("=" * 70)


if __name__ == "__main__":
    main()
