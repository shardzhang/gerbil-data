# MobileRec 数据集



存在 App 推荐公开数据集，但**没有和 Wide&Deep 论文同源、带曝光 + 安装标签的工业级真实流式数据集**（Google Play 真实用户曝光安装数据不对外公开）。



## 一、最适合复现 App 推荐排序模型（用户交互序列 + 评分 / 安装标签）

### 1. MobileRec（首选，Google Play 用户行为数据集）

- 规模：70 万独立用户、1.93 千万条用户交互（评论 / 评分）、1 万款 App、48 个类目；每个用户至少交互 5 款 App，自带**时序行为序列**，完美适配 DIN/Wide&Deep 序列建模
- 数据内容：用户时序行为、评分（可转为 0/1 安装二分类标签）、App 完整元数据（分类、描述、评分）、评论情感特征
- 获取地址：Hugging Face `recmeapp/mobilerec`，完全公开免费
- 适配任务：App 安装预估、序列推荐、Wide&Deep / DeepFM / DIN 复现





# MobileRec 数据集完整下载&加载教程
数据集官方仓库：`recmeapp/mobilerec`，托管在 Hugging Face Datasets，国内可通过镜像加速下载，分**一键Python加载**、**命令行批量下载**、**网页手动下载**三种方式。

## 前置依赖安装
先安装必备库
```bash
pip install datasets huggingface_hub pandas
```

## 方式1：Python代码一键加载（推荐，最简单）
### 国内加速配置（解决访问慢/超时）
在代码最顶部设置HF镜像环境变量，不用魔法也能高速下载：
```python
import os
# 配置国内镜像站
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"

from datasets import load_dataset

# 一行加载MobileRec数据集
dataset = load_dataset("recmeapp/mobilerec")

# 查看数据结构
print(dataset)
# 取训练集前5条样本
train_data = dataset["train"]
print(train_data[:5])

# 转成pandas DataFrame方便做Wide&Deep特征工程
df = train_data.to_pandas()
print(df.columns)
```
运行后会自动下载全部数据并缓存到本地，下次运行直接读取缓存，无需重复下载。

### 字段说明（适配App推荐建模）
- `user_id`：用户ID
- `app_id`：应用ID
- `rating`：评分（可二分类：≥4=安装正样本1，≤3=未安装0，模拟论文`app acquisition`标签）
- `review_date`：交互时间戳，构造用户历史行为序列
- `category`：App分类（用于构造Wide交叉特征 `user_install_category & impression_category`）
- `review_text`：评论文本（可选特征）

## 方式2：命令行 huggingface-cli 完整下载全部文件
适合需要本地原始CSV文件，离线使用场景
1. 设置镜像环境变量
```bash
# Linux/Mac
export HF_ENDPOINT=https://hf-mirror.com

# Windows PowerShell
$env:HF_ENDPOINT = "https://hf-mirror.com"
```
2. 执行下载命令，保存到本地`./mobile_rec_data`文件夹
```bash
huggingface-cli download datasets/recmeapp/mobilerec --local-dir ./mobile_rec_data
```
下载完成后本地会存放原始分片数据文件。

## 方式3：网页手动下载（无需代码，适合少量文件）
1. 打开国内镜像地址：https://hf-mirror.com/datasets/recmeapp/mobilerec
2. 页面右侧点击 **Files and versions**
3. 列表中所有`.parquet`分片文件可直接点击下载，parquet是高效列式存储格式，pandas可直接读取。

## 常见问题解决
### 1. 下载速度极慢、频繁断开
必须添加环境变量 `HF_ENDPOINT=https://hf-mirror.com`，使用国内镜像加速，不要直连huggingface.co。

### 2. 磁盘空间不足
MobileRec共1930万条交互数据，完整占用约2~3GB，提前预留存储空间。

### 3. 只加载部分数据（流式加载，低内存）
数据量大内存不足时，用流式加载，不一次性载入全部：
```python
import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
from datasets import load_dataset

# streaming=True 流式迭代，不占内存
dataset = load_dataset("recmeapp/mobilerec", streaming=True, split="train")
# 逐条遍历
for sample in dataset:
    print(sample)
    break
```

## 配套Wide&Deep数据预处理思路
1. 用`rating`构造二分类标签：label = 1 if rating >=4 else 0
2. 按`user_id + review_date`排序，生成用户历史安装App序列（Deep分支Embedding池化输入）
3. 手工构造Wide交叉特征：`user_install_app:impression_app`、`user_category:impression_category`，完全对齐Google Play论文特征形式。





# MobileRec 数据集完整详细介绍
## 一、基础背景与来源
### 1. 论文出处
SIGIR 2023 公开数据集论文：**MobileRec: A Large-Scale Dataset for Mobile Apps Recommendation**
作者团队构建该数据集的核心目的：填补学术界缺少**Google Play真实App推荐时序交互数据集**的空白。
传统推荐数据集（MovieLens、Amazon评论）偏向电商/影视，专门面向手机应用商店、带完整用户时序安装行为的公开数据极少，工业界Google Play内部曝光安装日志完全不对外开源，MobileRec是目前最贴合Wide&Deep论文场景的公开替代数据集。

### 2. 数据来源
原始数据采集自**Google Play商店用户公开评论交互记录**，全部为公开可爬取的用户App评价行为，无隐私敏感数据，遵循CC BY-SA 4.0开源协议，可商用、科研复现。

## 二、核心规模统计（官方标准数据）
| 指标 | 数值 | 说明 |
|------|------|------|
| 用户交互总条数 | 1930万（19.3M） | 每条=1条用户对单App的评论打分行为，等价于一次App安装交互 |
| 独立用户数量 | 70万（0.7M） | 严格过滤：**每个用户至少交互5款不同App**，不存在单交互冷启动用户 |
| 独立App总数 | 10000+ | 一万余款安卓应用 |
| App类目总数 | 48大类 | 完整复刻Google Play官方分类（社交、游戏、工具、办公、摄影等） |
| 存储格式 | Parquet分片 | Hugging Face托管，总占用磁盘2~3GB |

### 对比旧App数据集优势
早年App数据集大多单用户仅1条交互，无法构建用户历史行为序列；MobileRec天然支持**序列推荐建模**（DIN/DIEN/Wide&Deep Deep分支池化逻辑），完美复现论文中「用户历史安装App序列」特征。

## 三、完整字段详解（适配Wide&Deep建模）
数据集分为两大块：**用户-App交互行为表（核心训练样本）** + **App静态元数据表**
### 1. 交互表单条样本字段（对应论文中一条impression曝光样本）
| 字段名 | 含义 | Wide&Deep建模用途 |
|--------|------|-------------------|
| `user_id` | 用户唯一标识ID | 构造用户侧类别特征、用户历史序列分组 |
| `app_id` | 当前交互应用ID（候选曝光App） | 目标物品特征，Wide交叉核心项 |
| `rating` | 用户打分，1~5分 | 二分类标签构造：模拟论文`app acquisition`安装标签<br>label=1（4/5分，成功安装正样本）；label=0（1/2/3分，未满意安装负样本） |
| `review_date` | 评论时间戳（时序） | 按时间排序生成**用户历史安装序列**，Deep分支输入 |
| `review_text` | 用户评论文本 | 可选文本特征，不参与基础Wide&Deep |
| `sentiment` | 评论情感极性（正向/负向） | 辅助连续特征 |

### 2. App静态元数据字段（用于构造交叉特征）
每一条`app_id`绑定完整物品特征，完全对齐Google Play元数据：
1. `category`：App所属一级类目（48种，核心类别特征）
2. `app_name`：应用名称
3. `overall_rating`：App全局平均评分（连续特征，可分位数归一化）
4. `install_count`：商店总安装量（连续数值特征）
5. `size`：App安装包大小（连续特征）
6. `price`：是否付费应用（0/1二值特征）



## 四、数据核心特性（贴合Wide&Deep论文场景）

### 1. 天然支持Wide侧人工交叉特征工程
可直接复现论文所有交叉变换：
- 二阶交叉1：`user历史安装app_id & 当前曝光app_id`（AND组合，论文核心Wide特征）
- 二阶交叉2：`用户历史类目 & 当前App类目`
- 连续特征：安装量、评分做分位数离散化（论文4.1数据生成章节归一化逻辑）

### 2. 完整时序用户行为，适配Deep分支序列建模
每条样本带时间戳，可按`user_id`分组、按时间升序排序，截取用户前N个历史交互App ID，拼接Embedding向量，复现原文Deep分支：
> 所有类别特征学习32维Embedding，拼接后送入三层ReLU全连接网络

### 3. 数据稀疏性匹配工业真实场景
- 用户、App ID为超高维稀疏类别特征；
- 90%用户仅交互少量App，存在大量冷门App（对应论文提到的**稀疏高秩交互矩阵**，完美复现纯DNN过度泛化缺陷），可直观对比Wide&Deep vs DNN线上指标差异。

## 五、数据集适用任务（完全覆盖你的研究需求）
1. **复现Wide&Deep模型**（最匹配场景）
   构造曝光-安装二分类任务，模拟Google Play商店首页推荐排序，复现论文A/B指标、FTRL+AdaGrad双优化器训练流程。
2. **序列推荐模型训练（DIN/DIEN/DeepFM/DCN）**
   利用用户时序行为构建用户兴趣序列，对比纯深度网络与Wide&Deep的优劣。
3. 特征工程流程复现
   复现论文完整数据流水线：词表构建、低频特征过滤、连续特征分位数归一化、交叉特征生成。
4. 冷启动、稀疏交互推荐算法研究
   数据集包含大量小众冷门App，适合研究稀疏场景下记忆+泛化平衡。

## 六、数据集局限（必须注意，和Google Play原生数据的区别）
1. **无真实曝光日志（impression）**
   论文原始数据是「曝光后是否安装」；MobileRec只有**用户主动评价行为**，没有未点击、未曝光负样本，只能用打分间接模拟安装标签。
2. 缺少用户画像特征
   无性别、语言、设备、地域等用户侧基础类别特征，无法构造`gender=女 & language=en`这类交叉特征，只能用App行为做替代。
3. 无实时流式训练数据
   是静态离线数据集，无法复现论文增量热启动、线上实时推理延迟测试。
4. 规模差距大
   论文训练样本5000亿条，MobileRec仅1930万，离线AUC提升幅度会小于真实线上流量。

## 七、基于MobileRec复现Wide&Deep标准数据预处理流程（对标论文4.1）
1. **标签生成**
   $$label = \begin{cases}1, & rating \ge4 \\0, & rating \le3\end{cases}$$
2. **时序序列构建**
   按`user_id + review_date`升序，对每条样本截取该用户**历史所有交互app_id**作为Deep侧输入序列。
3. **类别特征词表构建**
   统计`user_id、app_id、category`频次，过滤出现次数低于阈值的低频ID，生成ID映射词表。
4. **连续特征分位数归一化**
   `overall_rating、install_count`按分位数划分为离散区间，归一至0~nq。
5. **Wide交叉特征构造**
   人工生成二元交叉乘积特征：`user_history_app:target_app`、`user_history_category:target_category`。

## 八、获取渠道
1. Hugging Face托管地址（国内镜像加速）：`hf-mirror.com/datasets/recmeapp/mobilerec`
2. 加载方式：`datasets.load_dataset("recmeapp/mobilerec")`
3. 论文原文：https://arxiv.org/abs/2303.06588