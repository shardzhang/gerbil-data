---
# For reference on model card metadata, see the spec: https://github.com/huggingface/hub-docs/blob/main/datasetcard.md?plain=1
# Doc / guide: https://huggingface.co/docs/hub/datasets-cards
{}
---

# MobileRec 数据集卡片



## 数据集描述

- **主页** ：https://github.com/mhmaqbool/mobilerec
- **仓库**：https://github.com/mhmaqbool/mobilerec
- **论文**：MobileRec: A Large-Scale Dataset for Mobile Apps Recommendation
- **联系人**
  - M.H. Maqbool (hasan.khowaja@gmail.com)
  - Abubakar Siddique (abubakar.ucr@gmail.com)




### 数据集简介

MobileRec 是一个大规模应用推荐数据集。包含 **1930 万** 条用户-应用交互记录（5-core 数据集），交互按时间升序排列。共有 **70 万** 名用户（每人至少 5 条交互）和 **10173** 个应用。



### 支持任务

序列推荐 (Sequential Recommendation)



### 语言

英语



## 下载说明

`mobilerec_final.csv` 大小为 **4.1 GB**（使用 Git LFS 存储）。如果无法访问 HuggingFace Hub，可使用镜像站：

```bash
# 设置镜像端点
export HF_ENDPOINT=https://hf-mirror.com

# 通过 huggingface_hub 下载
pip install huggingface_hub
python3 -m huggingface_hub.commands.huggingface_cli download --repo-type dataset recmeapp/mobilerec --local-dir ./mobile_rec_data
```

使用 `aria2c` 多线程加速下载：

```bash
export HF_ENDPOINT=https://hf-mirror.com
aria2c -x 8 -s 8 -k 1M \
  "https://hf-mirror.com/datasets/recmeapp/mobilerec/resolve/main/interactions/mobilerec_final.csv?download=1"
```

或直接用 Python `requests` 下载：

```python
import requests
endpoint = "https://hf-mirror.com"
url = f"{endpoint}/datasets/recmeapp/mobilerec/resolve/main/interactions/mobilerec_final.csv"
resp = requests.get(url, stream=True)
with open("mobilerec_final.csv", "wb") as f:
    for chunk in resp.iter_content(chunk_size=8192):
        f.write(chunk)
```



## 如何使用数据集

```python
from datasets import load_dataset
import pandas as pd

# 加载交互数据和元数据
mbr_data = load_dataset('recmeapp/mobilerec', data_dir='interactions')
mbr_meta = load_dataset('recmeapp/mobilerec', data_dir='app_meta')

# 保存为 CSV 文件
mbr_data['train'].to_csv('./mbr_data.csv')

# 转换为 pandas DataFrame
mobilerec_df = pd.read_csv('./mbr_data.csv')

print(f'MobileRec 数据集共有 {len(mobilerec_df)} 条交互记录')
print(f'共有 {len(mobilerec_df["app_package"].unique())} 个唯一应用')
print(f'共有 {len(mobilerec_df["uid"].unique())} 个唯一用户')
print(f'共有 {len(mobilerec_df["app_category"].unique())} 个应用类别')
```



## 数据字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `app_package` | string | 应用包名 |
| `review` | string | 用户评论 |
| `rating` | int | 评分 (1-5) |
| `votes` | int | 有用票数 |
| `date` | string | 原始日期字符串 |
| `uid` | string | 用户唯一标识 |
| `formated_date` | string | 格式化日期 (YYYY-MM-DD) |
| `unix_timestamp` | float | Unix 时间戳 |
| `app_category` | string | 应用类别 |



## 数据拆分

数据集已按用户拆分：
- **训练集**: 前 3 个交互用于训练
- **验证集**: 第 4 个交互用于验证
- **测试集**: 第 5 个交互用于测试



## 引用

```bibtex
@inproceedings{maqbool2024mobilerec,
  title={MobileRec: A Large-Scale Dataset for Mobile Apps Recommendation},
  author={Maqbool, M.H. and Siddique, Abubakar},
  year={2024}
}
```
