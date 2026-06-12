# ML-1M Data Analysis Queries

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

-- Data statistics:

-- 1. Number of distinct users

-- 2. Number of distinct items

-- 3. Average number of items per user

-- 4. Top 10 most/least rated movies (movie_id, title, genres, rate: min/max/avg/count)

-- 5. Top 10 highest/lowest rated movies (movie_id, title, genres, rate: min/max/avg/count)

-- 6. Rating counts by occupation and age group

-- 7. Average rating by occupation and age group

-- 8. Rating date distribution (day, movie_cnt, user_cnt)
