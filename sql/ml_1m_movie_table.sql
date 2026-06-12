-- name: shard zhang
-- date: 2026/6/1 11:32
-- description: Hive DDL for ML-1M movies table

-- 1. Create database ml_1m_db
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. Create movies table (from movies.dat)
-- CREATE TABLE IF NOT EXISTS ml_1m_db.movies
-- (
--     movie_id STRING comment 'movie ID',
--     title    STRING comment 'movie title',
--     genres   STRING comment 'genres'
-- )
-- comment 'movies table'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- Use RegexSerDe to support multi-character delimiter
drop table if exists ml_1m_db.movies;
create table if not exists ml_1m_db.movies
(
    movie_id STRING comment 'movie ID',
    title    STRING comment 'movie title',
    genres   STRING comment 'genres'
)
comment 'movies table'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
    'input.regex' = '^(.*?)::(.*?)::(.*?)$'
)
stored as textfile;

-- 3. Load local data into table
LOAD DATA LOCAL INPATH '/path/to/ml-1m/movies.dat'
OVERWRITE INTO TABLE ml_1m_db.movies;

-- 4. Verify import
-- Show table schema
DESC ml_1m_db.movies;
-- Show first 10 rows
SELECT * FROM ml_1m_db.movies LIMIT 10;
-- Count total rows (expected: 3883)
SELECT COUNT(*) FROM ml_1m_db.movies;
