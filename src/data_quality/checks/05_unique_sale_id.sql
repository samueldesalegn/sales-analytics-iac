-- Uniqueness: sale_id is the key and must have no duplicates.
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM (
    SELECT sale_id
    FROM sales_curated
    GROUP BY sale_id
    HAVING COUNT(*) > 1
);
