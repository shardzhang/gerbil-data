# Hive Common Commands

## 1. Enter Hive CLI

```bash
# Method 1: Run directly (non-interactive)
hive -e "select 1;"

# Method 2: Enter interactive mode (recommended for multiple commands)
hive
```

```bash
hive -e "set hive.execution.engine;"
# hive.execution.engine=mr
# hive.execution.engine=tez
```

## 2. Execute SQL directly (non-interactive)
```bash
hive -e "show databases;"
```

## 3. Execute SQL from file
```bash
hive -f test.sql
```

## 4. Show all databases
```bash
hive -e "show databases;"
```

## 5. Create database
```bash
hive -e "create database if not exists ml_1m_db;"
```

## 6. Drop database
```bash
hive -e "drop database if exists ml_1m_db cascade;"
```

## 7. Show all tables
```bash
hive -e "use ml_1m_db; show tables;"
```

## 8. Drop table
```bash
hive -e "drop table if exists ml_1m_db.ratings;"
```

## 9. Create table (ML-1M example)
```bash
hive -e "
drop table if exists ml_1m_db.ratings;
create table if not exists ml_1m_db.ratings
(
    user_id  string comment 'user ID',
    movie_id string comment 'movie ID',
    rating   int    comment 'rating',
    ts       bigint comment 'timestamp'
)
comment 'user ratings table'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
  'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)$'
)
stored as textfile;
"
```

## 10. Load local file into Hive table
```bash
hive -e "
load data local inpath '/path/to/ml-1m/ratings.dat'
overwrite into table ml_1m_db.ratings;
"
```

## 11. Query data
```bash
hive -e "select * from ml_1m_db.ratings limit 10;"
```

## 12. Count records
```bash
hive -e "select count(*) from ml_1m_db.ratings;"
hive --hiveconf hive.execution.engine=mr -e "select count(*) from ml_1m_db.ratings;"
hive -e "set hive.execution.engine=mr; select count(*) from ml_1m_db.ratings;"
```

## 13. Describe table schema
```bash
hive -e "desc ml_1m_db.ratings;"
```

## 14. Exit Hive
```bash
quit;
```
