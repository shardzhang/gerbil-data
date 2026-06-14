# 数据查看

ml-1m

数据为匿名化处理，共包含 6040 名用户对约 3900 部电影的 1,000,209 条评分；

## movies.dat

电影信息

`MovieID::Titile::Genres`

- MovieID：电影ID，范围1-3952
- Title：电影名称（含上映年份）
- `Genres`：类型，通过`|`分隔。Action、Adventure、Animation、Children's、Comedy、Crime、Documentary、Drama、Fantasy、Film-Noir、Horror、Musical、Mystery、Romance、Sci-Fi、Thriller、War、Western

```bash
~/Py/data/ml-1m  head movies.dat
1::Toy Story (1995)::Animation|Children's|Comedy
2::Jumanji (1995)::Adventure|Children's|Fantasy
3::Grumpier Old Men (1995)::Comedy|Romance
4::Waiting to Exhale (1995)::Comedy|Drama
5::Father of the Bride Part II (1995)::Comedy
6::Heat (1995)::Action|Crime|Thriller
7::Sabrina (1995)::Comedy|Romance
8::Tom and Huck (1995)::Adventure|Children's
9::Sudden Death (1995)::Action
10::GoldenEye (1995)::Action|Adventure|Thriller
```

## users.dat

格式：`UserID::Gender::Age::Occupation::Zip-code`

- 用户 ID，范围 1 - 6040

- 性别：M（男性）/ F（女性）

- Age：年龄段（编码对应）：

  1 → 18 岁以下

  18 → 18-24 岁

  25 → 25-34 岁

  35 → 35-44 岁

  45 → 45-49 岁

  50 → 50-55 岁

  56 → 56 岁以上

- Occupation：职业（编码对应）：

  0 → 其他 / 未说明

  1 → 学术 / 教育工作者

  2 → 艺术家

  3 → 文书 / 行政

  4 → 大学生 / 研究生

  5 → 客户服务

  6 → 医生 / 医疗保健

  7 → 高管 / 管理

  8 → 农民

  9 → 家庭主妇

  10 → K-12 学生

  11 → 律师

  12 → 程序员

  13 → 退休

  14 → 销售 / 营销

  15 → 科学家

  16 → 自雇

  17 → 技术人员 / 工程师

  18 → 工匠 / 技工

  19 → 失业

  20 → 作家

- 邮政编码（用户自愿提供，无准确性校验）

```bash
 ~/Py/data/ml-1m  head users.dat
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
4::M::45::7::02460
5::M::25::20::55455
6::F::50::9::55117
7::M::35::1::06810
8::M::25::12::11413
9::M::25::17::61614
10::F::35::1::95370
```

## ratings.dat

格式：`UserID::MovieID::Rating:Timestamp`

- `UserID`：用户 ID，范围 1 - 6040
- `MovieID`：电影 ID，范围 1 - 3952
- `Rating`：评分，仅支持 5 星制（整数，1-5 星）
- `Timestamp`：时间戳，单位为秒（自 Unix 纪元起）

补充：每个用户至少有 20 条评分记录

```bash
~/Py/data/ml-1m  head ratings.dat
1::1193::5::978300760
1::661::3::978302109
1::914::3::978301968
1::3408::4::978300275
1::2355::5::978824291
1::1197::3::978302268
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
1::919::4::978301368
```

二分类问题：将用户点击率建模为点击概率。令评分>=3设为正样本。评分<3设为负样本。

数据表：

1. ml_1m_item_tabel：物品表
2. ml_1m_user_tabel：用户表
3. ml_1m_label_tabel：标签表
4. ml_1m_user_behavior_tabel：用户行为序列表
5. ml_1m_train_sample_binary：样本表（二分类）
6. ml_1m_train_sample_multiclass：样本表（多分类）
