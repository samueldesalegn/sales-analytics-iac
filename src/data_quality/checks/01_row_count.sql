-- Completeness: the curated table must not be empty.
SELECT COUNT(*) > 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated;
