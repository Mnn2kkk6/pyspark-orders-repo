# upload_to_minio.py
# ---------------------------------------------------------------------------
# Tạo bucket "lakehouse-demo" trên MinIO (nếu chưa có), rồi upload đúng 3 file
# CSV sạch đã ghi ở local (output/bronze/orders.csv, output/silver/orders.csv,
# output/gold/order_summary.csv) lên đúng path:
#   bronze/orders, silver/orders, gold/order_summary
#
# Yêu cầu: đã chạy `docker compose up -d` để có MinIO đang chạy ở localhost:9000,
# và đã chạy xong bronze_layer.py, silver_layer.py, gold_layer.py trước đó.
#
# Cấu hình kết nối đọc từ biến môi trường, có default khớp docker-compose.yml
# đi kèm — chỉnh biến môi trường nếu bạn dùng MinIO ở nơi khác.
# ---------------------------------------------------------------------------

import os
from pathlib import Path

from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")             
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")             #tài khoản đăng nhập MinIO
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")          #mật khẩu đăng nhập MinIO
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

BUCKET_NAME = "lakehouse-demo"

# Map: file local (đúng 1 file/layer) -> object key (path) trên MinIO
FILES_TO_UPLOAD = {
    "output/bronze/orders.csv": "bronze/orders.csv",
    "output/silver/orders.csv": "silver/orders.csv",
    "output/gold/order_summary.csv": "gold/order_summary.csv",
}


def get_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    # Kiểm tra bucket đã tồn tại chưa, nếu chưa thì tạo mới — luôn kiểm tra
    # trước khi tạo để chạy lại script nhiều lần không bị lỗi.
    if client.bucket_exists(bucket_name):
        print(f"Bucket '{bucket_name}' da ton tai.")
    else:
        client.make_bucket(bucket_name)
        print(f"Da tao bucket moi: '{bucket_name}'")


def main() -> None:
    client = get_client()

    print(f"Ket noi MinIO tai: {MINIO_ENDPOINT}")
    try:
        ensure_bucket(client, BUCKET_NAME)
    except S3Error as e:
        print(f"Loi khi tao/kiem tra bucket: {e}")
        print("Kiem tra lai MinIO da chay chua (docker compose up -d) va thong tin dang nhap.")
        return

    print()
    for local_file, object_key in FILES_TO_UPLOAD.items():
        local_path = Path(local_file)
        if not local_path.exists():
            print(f"[BO QUA] Khong tim thay file: {local_file} "
                  f"(hay chay du bronze_layer.py / silver_layer.py / gold_layer.py chua?)")
            continue

        client.fput_object(BUCKET_NAME, object_key, str(local_path))
        print(f"Da upload: {local_file}  ->  s3://{BUCKET_NAME}/{object_key}")

    print("\nHoan tat upload Bronze/Silver/Gold len MinIO.")
    print(f"Xem tren web console: http://localhost:9001  (bucket: {BUCKET_NAME})")


if __name__ == "__main__":
    main()
