# create_orders_csv.py
# Mục đích: sinh ra file orders.csv mẫu để bước sau PySpark đọc vào xử lý
# Đây KHÔNG phải là Spark, chỉ là bước chuẩn bị dữ liệu đầu vào bằng Python thuần.

import csv
import random

# Danh sách tỉnh/thành mẫu để dữ liệu group by cho ra nhiều nhóm
provinces = ["Hanoi", "HCM", "DaNang", "HaiPhong", "CanTho"]
statuses = ["SUCCESS", "PENDING", "CANCELLED", "FAILED"]

random.seed(42)  # cố định seed để dữ liệu sinh ra giống nhau mỗi lần chạy (dễ kiểm tra kết quả)

rows = []
for order_id in range(1, 201):  # sinh 200 đơn hàng
    customer_id = f"CUS{random.randint(1, 50):03d}"
    province = random.choice(provinces)
    amount = round(random.uniform(50_000, 5_000_000), 0)  # số tiền VND ngẫu nhiên
    status = random.choices(statuses, weights=[0.6, 0.2, 0.1, 0.1])[0]  # 60% là SUCCESS
    rows.append([order_id, customer_id, province, int(amount), status])

# Ghi ra file CSV có header, đây là file mà bước PySpark sẽ đọc vào
with open("orders.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "province", "amount", "status"])
    writer.writerows(rows)

print(f"Đã tạo orders.csv với {len(rows)} dòng dữ liệu.")
