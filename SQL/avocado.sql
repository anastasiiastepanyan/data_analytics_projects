# Examples of SQL queries using window functions based on Avocado Prices dataset.

# 1. How many organic avocados have been sold in total by the end of each week (cumulative total sales)
# in New York and Los Angeles since the start of the observation period (04/01/15)?

SELECT
    region,
    date,
    total_volume,
    SUM(total_volume) OVER w AS volume
FROM avocado
WHERE region IN ('NewYork', 'LosAngeles')
    AND type = 'organic'
WINDOW w AS (
    PARTITION BY region
    ORDER BY date ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
ORDER BY region DESC, date ASC

# 2. Calculate the difference between n week sales (total_volume)
# and the number of conventional avocados sold during the previous week.
# Write the values in the new week_diff column.

# type - type of avocado (conventional)
# region - region (TotalUS)
# total_volume - weekly sales volume

SELECT
    date,
    total_volume,
    region,
    type,
    total_volume - LAG(total_volume, 1) OVER w AS week_diff
FROM avocado
WHERE region = 'TotalUS'
    AND type = 'conventional'
WINDOW w AS (
    ORDER BY date ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
ORDER BY region DESC, date ASC

# 3. Calculate the moving average price of avocados (average_price) in New York,
# broken down by type of avocado. Use the current week and the previous two as a window.

SELECT
    date,
    average_price,
    region,
    type,
    AVG(average_price) OVER w AS rolling_price
FROM avocado
WHERE region = 'NewYork'
WINDOW w AS (
    PARTITION BY type
    ORDER BY date ASC
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
