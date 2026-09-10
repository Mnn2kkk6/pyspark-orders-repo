# create_orders_dirty.py
# ---------------------------------------------------------------------------
# Sinh file orders.csv gồm cả dữ liệu đúng lẫn dữ liệu lỗi. Các loại lỗi được chèn vào:
#   - order_id rỗng (mô phỏng lỗi thu thập dữ liệu / hệ thống nguồn)
#   - amount <= 0 (đơn hàng lỗi, hoàn tiền âm, hoặc lỗi nhập liệu)
#   - status viết hoa/thường lẫn lộn: success, SUCCESS, Success, failed, FAILED...
# ---------------------------------------------------------------------------

import csv
import random
from datetime import date, timedelta

provinces = ["Hanoi", "HCM", "DaNang", "HaiPhong", "CanTho"]

# Cố ý liệt kê nhiều biến thể viết hoa/thường khác nhau cho cùng 1 trạng thái,
# đây chính là lỗi thực tế Silver layer cần chuẩn hóa (uppercase) sau này.
status_variants = {
    "SUCCESS": ["SUCCESS", "success", "Success"],
    "PENDING": ["PENDING", "pending", "Pending"],
    "CANCELLED": ["CANCELLED", "cancelled", "Cancelled"],
    "FAILED": ["FAILED", "failed", "Failed"],
}

random.seed(42)  # cố định seed để dữ liệu sinh ra giống nhau mỗi lần chạy

start_date = date(2026, 1, 1)


def random_order_date() -> str:
    offset = random.randint(0, 250)
    return (start_date + timedelta(days=offset)).isoformat()


rows = []

# ---- 220 dòng dữ liệu sạch (hợp lệ) ----
for order_id in range(1, 221):
    customer_id = f"CUS{random.randint(1, 50):03d}"
    province = random.choice(provinces)
    amount = random.randint(50_000, 5_000_000)
    status_group = random.choices(
        list(status_variants.keys()), weights=[0.55, 0.2, 0.1, 0.15]
    )[0]
    status = random.choice(status_variants[status_group])  # random hoa/thường
    order_date = random_order_date()
    rows.append([order_id, customer_id, province, amount, status, order_date])

# ---- 15 dòng lỗi: order_id rỗng ----
for _ in range(15):
    customer_id = f"CUS{random.randint(1, 50):03d}"
    province = random.choice(provinces)
    amount = random.randint(50_000, 5_000_000)
    status = random.choice(status_variants["SUCCESS"])
    order_date = random_order_date()
    rows.append(["", customer_id, province, amount, status, order_date])  # order_id rỗng

# ---- 15 dòng lỗi: amount <= 0 ----
next_id = 221
for i in range(15):
    customer_id = f"CUS{random.randint(1, 50):03d}"
    province = random.choice(provinces)
    amount = random.choice([0, -1, -50000])
    status = random.choice(status_variants["SUCCESS"])
    order_date = random_order_date()
    rows.append([next_id + i, customer_id, province, amount, status, order_date])

# Trộn ngẫu nhiên thứ tự các dòng để dữ liệu lỗi không nằm dồn cục ở cuối file
random.shuffle(rows)

with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "province", "amount", "status", "order_date"])
    writer.writerows(rows)

print(f"Da tao orders.csv voi {len(rows)} dong (bao gom du lieu loi co tinh).")
