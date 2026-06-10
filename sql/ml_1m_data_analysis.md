
```bash
hive -e "
set hive.auto.convert.join=false;
SELECT
    u.user_id,
    r.rating
FROM ml_1m_db.ratings r
inner JOIN ml_1m_db.users u
ON r.user_id = u.user_id
LIMIT 20;
"
```




-- 数据统计：

-- 1、多少个不同用户

-- 2、多少个不同item

-- 3、每个用户包含的平均item个数

-- 4、评分最多/少的Top10电影(movie_id, title, genres, rate: min/max/avg/count)

-- 5、评分最高/低的Top10电影(movie_id, title, genres, rate: min/max/avg/count)

-- 6、 各个职业、年龄段的电影评分个数

-- 7、 各个职业、年龄段的电影平均评分

-- 8、评分的日期分布(day, movie_cnt, user_cnt, )

--

