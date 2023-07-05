# For examples of SQL queries, data from the Airbnb platform in Berlin is used.

# 1. When (month, year) the most number of new hosts were registered?

SELECT
    COUNT(DISTINCT host_id) AS num_hosts,
    toStartOfMonth(toDateOrNull(host_since)) AS date_since
FROM listings
GROUP BY date_since
ORDER BY num_hosts DESC
LIMIT 1

# 2. Calculate the average response rate broken down by whether the host is a superhost or not.

SELECT
    a.host_is_superhost AS host_is_superhost,
    AVG(toInt32OrNull(replaceAll(a.host_response_rate, '%', ''))) AS avg_host_resp_rate
FROM (
    SELECT
        DISTINCT host_id,
        host_is_superhost,
        host_response_rate
    FROM
        listings
    ) AS a
GROUP BY host_is_superhost
ORDER BY avg_host_resp_rate DESC
LIMIT 100

# 3. What price per night do hosts set on average (one host can have several ads)?

SELECT
    host_id,
    groupArray(id) AS id,
    AVG(toFloat32OrNull(replaceRegexpAll(price, '[$,]', ''))) AS avg_price
FROM
    listings
GROUP BY host_id
ORDER BY avg_price DESC,
    host_id DESC
LIMIT 100

# 4. Сalculate the difference between the maximum and the minimum prices for each host.

SELECT
    host_id,
    groupArray(id) AS id,
    MAX(toFloat32OrNull(replaceRegexpAll(price, '[$,]', ''))) - MIN(toFloat32OrNull(replaceRegexpAll(price, '[$,]', ''))) AS price_diff
FROM
    listings
GROUP BY host_id
ORDER BY price_diff DESC
LIMIT 100

# 5. Group the data by room_type and print averages of the price per night, the security_deposit, and the cleaning_fee.
# For which room_type is the average security_deposit value the highest?

SELECT
    room_type,
    AVG(toFloat32OrNull(replaceRegexpAll(price, '[$,]', ''))) AS avg_price,
    AVG(toFloat32OrNull(replaceRegexpAll(security_deposit, '[$,]', ''))) AS avg_sec_dep,
    AVG(toFloat32OrNull(replaceRegexpAll(cleaning_fee, '[$,]', ''))) AS avg_clean_fee
FROM
    listings
GROUP BY room_type
ORDER BY avg_sec_dep DESC
LIMIT 1

# 6. In which Berlin neighbourhoods is the average area of housing that is 'Entire home/apt' the largest? Sort by average and pick top 3.

SELECT
    neighbourhood_cleansed,
    AVG(toFloat32OrNull(square_feet)) AS avg_square_feet
FROM listings
WHERE room_type = 'Entire home/apt'
GROUP BY neighbourhood_cleansed
ORDER BY avg_square_feet DESC
LIMIT 3

# 7. Which room ('Private room') is the closest to the city center?
# 52.5200 N, 13.4050 E - Berlin's center coordinates.

SELECT
    id,
    geoDistance(13.4050, 52.5200, toFloat64OrNull(longitude), toFloat64OrNull(latitude)) AS distance
FROM listings
WHERE room_type = 'Private room'
GROUP BY id, latitude, longitude
ORDER BY distance
LIMIT 1

# 8. Keep only the ads that have an above-average review_score_rating and strictly less than three reviews_per_month.

SELECT
    id,
    toFloat64OrNull(review_scores_rating) AS review_scores_rating,
    reviews_per_month
FROM
    listings
WHERE
    review_scores_rating > (
                            SELECT
                                AVG(toFloat64OrNull(review_scores_rating))
                            FROM
                                listings
                            )
    AND reviews_per_month < 3
GROUP BY id, reviews_per_month, review_scores_rating
ORDER BY reviews_per_month DESC, review_scores_rating DESC
LIMIT 100

# 9. Calculate the average distance to the city center and display the IDs of the ads
# for renting individual rooms for which the distance turned out to be less than the average.
# Choose the room that is the most distant from the center,
# but at the same time located closer than the rest of the rooms on average.

WITH
    (SELECT AVG(geoDistance(13.4050, 52.5200, toFloat64OrNull(longitude),
    toFloat64OrNull(latitude)))
    FROM listings
    WHERE room_type = 'Private room') AS avg_distance

SELECT
    host_id,
    geoDistance(13.4050, 52.5200, toFloat64OrNull(longitude), toFloat64OrNull(latitude)) AS distance
FROM listings
WHERE distance < avg_distance
    AND room_type = 'Private room'
GROUP BY host_id, distance
ORDER BY distance DESC
LIMIT 1

# 10. You plan to rent accommodation in Berlin for 7 days, using more sophisticated filters than those offered on the website.

# Select ads from the listings table that:
# 1) are at a distance from the center less than the average.
# 2) cost less than $100 per day
# 3) have last reviews (last_review) since September 1, 2018
# 4) have WiFi in the list of amenities (amenities)

WITH
    (SELECT AVG(geoDistance(13.4050, 52.5200, toFloat64OrNull(longitude),
    toFloat64OrNull(latitude)))
    FROM listings
    ) AS avg_distance

SELECT
    host_id
FROM listings
WHERE geoDistance(13.4050, 52.5200, toFloat64OrNull(longitude), toFloat64OrNull(latitude)) < avg_distance
    AND toFloat32OrNull(replaceRegexpAll(price, '[$,]', '')) + toFloat32OrNull(replaceRegexpAll(cleaning_fee, '[$,]', ''))/7 < 100
    AND toStartOfMonth(toDateOrNull(last_review)) >= '2018-09-01'
    AND multiSearchAnyCaseInsensitive(amenities, ['Wifi']) != 0
ORDER BY toFloat64OrNull(replaceAll(review_scores_rating, ',', '.')) DESC
LIMIT 100
