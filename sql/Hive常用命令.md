# Hive 常用本地命令

1. 进入 Hive 命令行

```bash
# 方式1：直接在终端运行（不进入交互模式）
hive -e "select 1;"

# 方式2：进入 Hive 交互模式（推荐，方便后续操作）
hive
```

```bash
hive -e "set hive.execution.engine;" 
# hive.execution.engine=mr
# hive.execution.engine=tez
```

2. 直接执行 SQL（不进入交互模式）
```bash
hive -e "show databases;"
```

3. 执行文件里的 SQL
```bash
hive -f test.sql
```

4. 查看所有数据库
```bash
hive -e "show databases;"
```

5. 创建数据库
```bash
hive -e "create database if not exists ml_1m_db;"
```

6. 删除数据库
```bash
hive -e "drop database if exists ml_1m_db cascade;"
```

7. 查看所有表
```bash
hive -e "use ml_1m_db; show tables;"
```

13. 删除表
```bash
hive -e "drop table if exists ml_1m_db.ratings;"
```

8. 创建表（以 ml-1m 数据为例）
```bash
hive -e "
drop table if exists ml_1m_db.ratings;
create table if not exists ml_1m_db.ratings
(
    user_id  string comment '用户ID',
    movie_id string comment '电影ID',
    rating   int    comment '评分',
    ts       bigint comment '时间戳'
)
comment '用户评分表'
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
with serdeproperties (
  'input.regex' = '^(.*?)::(.*?)::(.*?)::(.*?)$'
)
stored as textfile;
"
```

9. 加载本地文件到 Hive 表
```bash
hive -e "
load data local inpath '/Users/dazhang/PycharmProject/data/ml-1m/ratings.dat'
overwrite into table ml_1m_db.ratings;
"
```

10. 查询数据
```bash
hive -e "select * from ml_1m_db.ratings limit 10;"
```

11. 统计表条数
```bash
hive -e "select count(*) from ml_1m_db.ratings;"
hive --hiveconf hive.execution.engine=mr -e "select count(*) from ml_1m_db.ratings;"
hive -e "set hive.execution.engine=mr; select count(*) from ml_1m_db.ratings;"
```

12. 查看表结构
```bash
hive -e "desc ml_1m_db.ratings;"
```

 14. 退出 Hive
```bash
quit;
``
