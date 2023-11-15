# 1
SELECT b.business_id, b.name
FROM business b
         JOIN (SELECT business_id, COUNT(DISTINCT user_id) AS user_count
               FROM visit
               GROUP BY business_id, user_id
               HAVING COUNT(user_id) >= 5) v ON b.business_id = v.business_id
WHERE b.state = 'NV'
GROUP BY b.business_id, b.name
HAVING COUNT(DISTINCT v.user_count) >= 1;

# 2
WITH CategoryCounts AS (SELECT c.category,
                               b.business_id,
                               b.name,
                               COUNT(b.business_id) OVER (PARTITION BY c.category) AS business_count
                        FROM business_category c
                                 LEFT JOIN
                             business b ON c.business_id = b.business_id)
SELECT category,
       business_id,
       name
FROM CategoryCounts
WHERE business_count = (SELECT MAX(business_count)
                        FROM CategoryCounts)
  AND business_id IN (SELECT business_id
                      FROM business
                      WHERE stars = 5);

# 3
SELECT b.name,
       b.address    as address,
       b.city,
       b.state,
       b.stars      AS stars,
       AVG(r.stars) AS avg_stars
FROM business b
         JOIN
     review r ON r.business_id = b.business_id
GROUP BY b.business_id, b.stars, b.city
HAVING AVG(r.stars) > b.stars
   AND b.city = 'Santa Barbara'
   AND b.stars > 4;

# 4
select A.friend_name, A.friendVisitCount
from (SELECT
#     u.user_id                      AS user_id,
#        u.name                         AS user_name,
COUNT(DISTINCT v1.business_id) AS johnVisitCount,
#        f.friend_id,
u2.name                        AS friend_name,
COUNT(DISTINCT v2.business_id) AS friendVisitCount
      FROM user u
               JOIN
           friend f ON u.user_id = f.user_id
               JOIN
           user u2 ON f.friend_id = u2.user_id
               JOIN
           visit v1 ON v1.user_id = u.user_id
               JOIN
           business_category bc1 ON bc1.business_id = v1.business_id
               JOIN
           visit v2 ON v2.user_id = f.friend_id
               JOIN
           business_category bc2 ON bc2.business_id = v2.business_id
      WHERE u.name = 'John'
        AND bc1.category = 'Shopping'
        AND bc2.category = 'Shopping'
      GROUP BY u.user_id, u.name, f.friend_id, u2.name
      HAVING johnVisitCount < friendVisitCount) as A;

# 5
SELECT user_id,
       (compliment_hot + compliment_more + compliment_profile + compliment_cute + compliment_list + compliment_note +
        compliment_plain + compliment_cool + compliment_funny + compliment_writer + compliment_photos +
        100 * elite_count) AS influence_score
FROM compliment
         NATURAL JOIN
     (SELECT user_id, COUNT(elite_year) AS elite_count
      FROM user
               LEFT JOIN
           user_elite
           USING (user_id)
      GROUP BY user_id) AS A;

# 6
SELECT business_id, name
FROM business
WHERE business_id IN (SELECT B.business_id
                      FROM (SELECT business_id, COUNT(date) AS cnt
                            FROM checkin
                            WHERE YEAR(date) = 2019
                            GROUP BY business_id) AS A
                               RIGHT JOIN
                           (SELECT business_id, COUNT(date) AS cnt
                            FROM checkin
                            WHERE YEAR(date) = 2020
                            GROUP BY business_id) AS B
                           ON A.business_id = B.business_id
                      WHERE A.cnt IS NULL
                         OR A.cnt < B.cnt);

# 7
select b.business_id, b.name, avg(r.stars) as avg_rating
from business b
         join review r
              on b.business_id = r.business_id
where r.date > '2023-01-01 00:00:00'
  and b.city = 'Santa Barbara'
group by b.business_id, b.name
having avg_rating > 4
order by avg_rating desc;

# 8
select b.business_id, b.name, r.text
from business b
         join review r
              on b.business_id = r.business_id
where r.text like '%excellent service%';

# 9
select b.city, count(b.city) as num_5_star
from business b
where b.stars = 5
group by b.city
order by num_5_star desc
limit 1;

# 10
SELECT u.user_id,
       b.business_id,
       r.stars
FROM business b
         JOIN
     review r ON b.business_id = r.business_id
         JOIN
     user u ON r.user_id = u.user_id
WHERE b.city = 'Whitestown';

# 11
SELECT city, category, averageStars, businessCount
FROM (SELECT b.city,
             c.category,
             AVG(b.stars)                                                                      AS averageStars,
             COUNT(*)                                                                          AS businessCount,
             ROW_NUMBER() OVER (PARTITION BY b.city ORDER BY COUNT(*) DESC, AVG(b.stars) DESC) AS rowNum
      FROM business as b
               JOIN
           business_category as c ON b.business_id = c.business_id
      WHERE b.is_open = 1
      GROUP BY b.city, c.category) AS ranked_categories
WHERE businessCount > 10
  and rowNum = 1;

# 12
SELECT b.name, b.address, b.city, b.postal_code, b.stars
FROM business as b
WHERE b.city = 'Tucson'
  AND b.stars > 4
  AND b.is_open = 1
  AND (
              6371 * acos(
                          cos(radians(32.25346)) * cos(radians(latitude))
                          * cos(radians(longitude) - radians(-110.91179))
                      + sin(radians(32.25346)) * sin(radians(latitude))
              )
          ) <= 50
ORDER BY stars DESC;

