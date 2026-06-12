-- name: shard zhang
-- date: 2026/6/1 11:31
-- description: Hive DDL for ML-1M ratings table

-- 1. Create database ml_1m_db
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. Create ratings table (from ratings.dat)
-- drop table if exists ml_1m_db.ratings;
-- create table if not exists ml_1m_db.ratings
-- (
--     user_id  STRING comment 'user ID',
--     movie_id STRING comment 'movie ID',
--     rating   INT    comment 'rating',
--     ts       BIGINT comment 'timestamp'
-- )
-- comment 'user ratings table'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- Use RegexSerDe to support multi-character delimiter
drop table if exists ml_1m_db.ratings;
create table if not exists ml_1m_db.ratings
(
    user_id  STRING comment 'user ID',
    movie_id STRING comment 'movie ID',
    rating   INT    comment 'rating',
    ts       BIGINT comment 'timestamp'
)
comment 'user ratings table'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
    'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)$'
)
stored as textfile;

-- 3. Load local data into table
LOAD DATA LOCAL INPATH '/path/to/ml-1m/ratings.dat'
OVERWRITE INTO TABLE ml_1m_db.ratings;

-- 4. Verify import
-- Show table schema
DESC ml_1m_db.ratings;
-- Show first 10 rows
SELECT * FROM ml_1m_db.ratings LIMIT 10;
-- Count total rows (expected: 1000209)
SELECT COUNT(*) FROM ml_1m_db.ratings;
