-- name: shard zhang
-- date: 2026/6/1 11:31
-- description:

-- 1. 创建ml_1m_db库
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. 创建 ratings 评分表（对应 ratings.dat）
-- drop table if exists ml_1m_db.ratings;
-- create table if not exists ml_1m_db.ratings
-- (
--     user_id  STRING comment '用户ID',
--     movie_id STRING comment '电影ID',
--     rating   INT    comment '评分',
--     ts       BIGINT comment '时间戳'
-- )
-- comment '用户评分表'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- 支持分隔符为多符号
drop table if exists ml_1m_db.ratings;
create table if not exists ml_1m_db.ratings
(
    user_id  STRING comment '用户ID',
    movie_id STRING comment '电影ID',
    rating   INT    comment '评分',
    ts       BIGINT comment '时间戳'
)
comment '用户评分表'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
    'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)$'
)
stored as textfile;

-- 3. 加载本地数据到表中
LOAD DATA LOCAL INPATH '/Users/dazhang/PycharmProject/data/ml-1m/ratings.dat'
OVERWRITE INTO TABLE ml_1m_db.ratings;

-- 4. 校验是否导入成功
-- 查看表结构
DESC ml_1m_db.ratings;
-- 查看前 10 条数据
SELECT * FROM ml_1m_db.ratings LIMIT 10;
-- 统计表总条数
SELECT COUNT(*) FROM ml_1m_db.ratings; -- 应该是 1000209 条
