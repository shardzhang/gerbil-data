# Hive 常用本地命令



#### 1. 进入 Hive 命令行
```bash
hive
```



#### 2. 直接执行 SQL（不进入交互模式）

```bash
hive -e "show databases;"
```



#### 3. 执行文件里的 SQL

```bash
hive -f test.sql
```



#### 4. 查看所有数据库

```bash
hive -e "show databases;"
```



#### 5. 创建数据库

```bash
hive -e "create database if not exists ml_1m_db;"
```



#### 6. 删除数据库

```bash
hive -e "drop database if exists ml_1m_db cascade;"
```



#### 7. 查看所有表

```bash
hive -e "use ml_1m_db; show tables;"
```



#### 8. 创建表（以 ml-1m 数据为例）

```bash
hive -e "
create table if not exists ml_1m_db.ratings (
    user_id int,
    movie_id int,
    rating int,
    ts bigint
)
row format delimited
fields terminated by '::'
stored as textfile;
"
```



#### 9. 加载本地文件到 Hive 表

```bash
hive -e "
load data local inpath '/Users/dazhang/PycharmProject/data/ml-1m/ratings.dat'
overwrite into table ml_1m_db.ratings;
"
```



#### 10. 查询数据

```bash
hive -e "select * from ml_1m_db.ratings limit 10;"
```



#### 11. 统计表条数

```bash
hive -e "select count(*) from ml_1m_db.ratings;"
```


#### 12. 查看表结构

```bash
hive -e "desc ml_1m_db.ratings;"
```



#### 13. 删除表

```bash
hive -e "drop table if exists ml_1m_db.ratings;"
```



#### 14. 退出 Hive

```
quit;
```

```bash
hive -e "show databases;"
hive -e "show tables;"
hive -e "select * from ml_1m_db.ratings limit 5;"
hive -e "desc test_db.ratings;"
```

---

需要我给你 **一套直接跑通 ml-1m 数据的 Hive 建表 + 导入完整命令** 吗？
你复制就能跑，直接把电影评分数据导入 Hive 里用！





来！直接给你一套**开箱即用、复制就跑**的完整命令，一次性把 ml-1m 数据建表、导入、查询搞定！

---

#### 1. 准备：先启动 Hive（可选）
先确保你的 Hive 环境正常，下面两种方式二选一：
```bash
# 方式1：直接在终端运行（不进入交互模式）
hive -e "select 1;"

# 方式2：进入 Hive 交互模式（推荐，方便后续操作）
hive
```

---



#### 2. 完整命令：建库、建表、导入数据

把下面这段直接复制运行（注意把 `PATH_TO_YOUR_DATA` 换成你本地 ml-1m 数据的实际路径）：

```sql
-- 1. 创建专用数据库（避免和 default 混在一起）
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. 创建 ratings 评分表（对应 ratings.dat）
CREATE TABLE IF NOT EXISTS ratings (
    user_id    INT,
    movie_id   INT,
    rating     INT,
    ts         BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '::'
STORED AS TEXTFILE;

-- 3. 创建 users 用户表（对应 users.dat）
CREATE TABLE IF NOT EXISTS users (
    user_id    INT,
    gender     CHAR(1),
    age        INT,
    occupation INT,
    zipcode    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '::'
STORED AS TEXTFILE;

-- 4. 创建 movies 电影表（对应 movies.dat）
CREATE TABLE IF NOT EXISTS movies (
    movie_id   INT,
    title      STRING,
    genres     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '::'
STORED AS TEXTFILE;

-- 5. 加载本地数据到表中（关键：替换成你自己的路径！）
-- 示例路径：/Users/dazhang/PycharmProject/data/ml-1m/ratings.dat
LOAD DATA LOCAL INPATH '/PATH_TO_YOUR_DATA/ratings.dat'
OVERWRITE INTO TABLE ratings;

LOAD DATA LOCAL INPATH '/PATH_TO_YOUR_DATA/users.dat'
OVERWRITE INTO TABLE users;

LOAD DATA LOCAL INPATH '/PATH_TO_YOUR_DATA/movies.dat'
OVERWRITE INTO TABLE movies;
```

---



#### 3. 验证数据是否导入成功

```sql
-- 查看表结构
DESC ratings;
DESC users;
DESC movies;

-- 查看前 10 条数据
SELECT * FROM ratings LIMIT 10;
SELECT * FROM users LIMIT 10;
SELECT * FROM movies LIMIT 10;

-- 统计表总条数（校验是否导入完整）
SELECT COUNT(*) FROM ratings;  -- 应该是 1000209 条
SELECT COUNT(*) FROM users;    -- 应该是 6040 条
SELECT COUNT(*) FROM movies;   -- 应该是 3883 条
```

---



#### 4. 附赠一个简单的多表关联查询

试试关联三张表，查用户的评分详情：
```sql
SELECT
    u.user_id,
    u.gender,
    m.title,
    r.rating,
    from_unixtime(r.ts, 'yyyy-MM-dd') AS rating_date
FROM ratings r
JOIN users u ON r.user_id = u.user_id
JOIN movies m ON r.movie_id = m.movie_id
LIMIT 20;
```

---



#### 补充说明

- 注意：`LOAD DATA LOCAL INPATH` 必须写**本地文件系统的绝对路径**，不能写相对路径。
- 导入成功后，你就可以在 PyCharm/IDEA 的 Hive 数据源里直接看到这些表，也能在 Spark 里用 `spark.sql("select * from ml_1m_db.ratings")` 直接读取。

---

如果你把路径发给我，我可以帮你把命令里的路径直接替换好，你复制过去就能直接跑，不用手动修改。