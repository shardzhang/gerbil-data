-- name: shard zhang
-- date: 2026/6/1 11:31
-- description: Hive DDL for ML-1M users table

-- 1. Create database ml_1m_db
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. Create users table (from users.dat)
-- CREATE TABLE IF NOT EXISTS ml_1m_db.users
-- (
--     user_id    string   comment 'user ID',
--     gender     string   comment 'gender (M=Male, F=Female)',
--     age        string   comment 'age group',
--     occupation string   comment 'occupation code',
--     zipcode    STRING   comment 'zip code'
-- )
-- comment 'users table'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- Use RegexSerDe to support multi-character delimiter
CREATE TABLE IF NOT EXISTS ml_1m_db.users
(
    user_id    STRING   comment 'user ID',
    gender     STRING   comment 'gender (M=Male, F=Female)',
    age        STRING   comment 'age group',
    occupation STRING   comment 'occupation code',
    zipcode    STRING   comment 'zip code'
)
    comment 'users table'
    row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
        with serdeproperties (
        'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)::(.*?)$'
        )
    stored as textfile;

-- 3. Load local data into table
LOAD DATA LOCAL INPATH '/path/to/ml-1m/users.dat'
OVERWRITE INTO TABLE ml_1m_db.users;

-- 4. Verify import
-- Show table schema
DESC ml_1m_db.users;
-- Show first 10 rows
SELECT * FROM ml_1m_db.users LIMIT 10;
-- Count total rows (expected: 6040)
SELECT COUNT(*) FROM ml_1m_db.users;
