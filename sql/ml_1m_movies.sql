-- name: shard zhang
-- date: 2026/6/1 11:32
-- description:

-- 1. 创建专用数据库（避免和 default 混在一起）
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. 创建 movies 电影表（对应 movies.dat）
-- CREATE TABLE IF NOT EXISTS ml_1m_db.movies
-- (
--     movie_id STRING comment '电影ID',
--     title    STRING comment '电影名称',
--     genres   STRING comment '类型'
-- )
-- comment '电影表'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- 支持分隔符为多符号
drop table if exists ml_1m_db.movies;
create table if not exists ml_1m_db.movies
(
    movie_id STRING comment '电影ID',
    title    STRING comment '电影名称',
    genres   STRING comment '类型'
)
comment '电影表'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
    'input.regex' = '^(.*?)::(.*?)::(.*?)$'
)
stored as textfile;

-- 3. 加载本地数据到表中
LOAD DATA LOCAL INPATH '/Users/dazhang/PycharmProject/data/ml-1m/movies.dat'
OVERWRITE INTO TABLE ml_1m_db.movies;

-- 4. 校验是否导入成功
-- 查看表结构
DESC ml_1m_db.movies;
-- 查看前 10 条数据
SELECT * FROM ml_1m_db.movies LIMIT 10;
-- 统计表总条数（校验是否导入完整）
SELECT COUNT(*) FROM ml_1m_db.movies; -- 应该是 3883 条
