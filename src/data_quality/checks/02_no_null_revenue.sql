-- Completeness: revenue must be populated on every row.
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated
WHERE revenue IS NULL;
