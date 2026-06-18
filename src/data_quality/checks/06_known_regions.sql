-- Domain: region must be one of the expected set.
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated
WHERE region NOT IN ('North', 'South', 'East', 'West');
