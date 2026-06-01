-- name: shard zhang
-- date: 2026/6/1 11:31
-- description:

-- 1. 创建专用数据库（避免和 default 混在一起）
CREATE DATABASE IF NOT EXISTS ml_1m_db;
USE ml_1m_db;

-- 2. 创建 users 用户表（对应 users.dat）
-- CREATE TABLE IF NOT EXISTS ml_1m_db.users
-- (
--     user_id    string   comment '用户ID',
--     gender     string   comment '性别M男性,F女性',
--     age        string   comment '年龄段',
--     occupation string   comment '职业',
--     zipcode    STRING   comment '邮政编码'
-- )
-- comment '用户表'
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '::'
-- STORED AS TEXTFILE;

-- 支持分隔符为多符号
CREATE TABLE IF NOT EXISTS ml_1m_db.users
(
    user_id    STRING   comment '用户ID',
    gender     STRING   comment '性别M男性,F女性',
    age        STRING   comment '年龄段',
    occupation STRING   comment '职业',
    zipcode    STRING   comment '邮政编码'
)
    comment '用户表'
    row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
        with serdeproperties (
        'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)::(.*?)$'
        )
    stored as textfile;

-- 3. 加载本地数据到表中
LOAD DATA LOCAL INPATH '/Users/dazhang/PycharmProject/data/ml-1m/users.dat'
OVERWRITE INTO TABLE ml_1m_db.users;

-- 4. 校验是否导入成功
-- 查看表结构
DESC ml_1m_db.users;
-- 查看前 10 条数据
SELECT * FROM ml_1m_db.users LIMIT 10;
-- 统计表总条数（校验是否导入完整）
SELECT COUNT(*) FROM ml_1m_db.users; -- 应该是 6040 条
