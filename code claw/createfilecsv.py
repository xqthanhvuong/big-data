import json
import pandas as pd
import os

folder_path = r"C:\Users\HP\OneDrive\Desktop\big data";

all_data = []

for i in range(0, 83):
    file_name = f"jobs_data_viecoi{i}.json"
    file_path = os.path.join(folder_path, file_name)

    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            all_data.extend(data)  # Gộp tất cả dữ liệu lại
    else:
        print(f"⚠️ File không tồn tại: {file_name}")

# file_name = f"jobs_data.json"
# file_path = os.path.join(folder_path, file_name)
# if os.path.exists(file_path):
#     with open(file_path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#         all_data.extend(data)
# else: 
#     print(f"⚠️ File không tồn tại: {file_name}")
#
# df = pd.DataFrame(all_data)
#
# output_path = os.path.join(folder_path, "all_jobs_data_vieclam24h.csv")
# df.to_csv(output_path, index=False, encoding="utf-8-sig")
#
# print("✅ Xuất file CSV thành công:", output_path)