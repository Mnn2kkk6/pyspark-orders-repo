# common.py
# ---------------------------------------------------------------------------
# Hàm tiện ích dùng chung cho cả 3 layer Bronze/Silver/Gold 
# nguyên hàm write_single_csv() đã viết ở bài PySpark trước (spark_orders.py),
# tách ra file riêng để bronze_layer.py / silver_layer.py / gold_layer.py
# đều import dùng chung .
# ---------------------------------------------------------------------------

import glob
import os
import shutil


def write_single_csv(df, final_path: str) -> None:
    """
    Ghi 1 DataFrame vào 1 file CSV có tên đã đặt (final_path).

    Spark mặc định ghi CSV ra 1 THƯ MỤC chứa nhiều file part-xxxx.csv (do xử lý
    song song nhiều partition). Với dữ liệu nhỏ , ta muốn có 1 file .csv sạch sẽ, tên rõ ràng để dễ mở/push git/upload MinIO, nên hàm này:
      1. coalesce(1)  -> gom về 1 partition duy nhất trước khi ghi
      2. ghi ra thư mục tạm
      3. tìm file part-*.csv trong thư mục tạm, đổi tên thành final_path
      4. xóa thư mục tạm
    """
    tmp_dir = final_path + "__tmp"

    # Xóa thư mục tạm cũ nếu còn sót từ lần trước
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    # Đảm bảo thư mục cha của final_path tồn tại (VD: output/bronze/)
    parent_dir = os.path.dirname(final_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    # Tìm file part-*.csv Spark vừa ghi ra trong thư mục tạm
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"Khong tim thay file part-*.csv trong {tmp_dir}")

    # Xóa file cũ nếu đã tồn tại, rồi move file part- vào đúng tên mong muốn
    if os.path.exists(final_path):
        os.remove(final_path)
    shutil.move(part_files[0], final_path)

    # Dọn thư mục tạm (bao gồm _SUCCESS, .crc... Spark sinh ra)
    shutil.rmtree(tmp_dir)

    print(f"Da ghi: {final_path}")
