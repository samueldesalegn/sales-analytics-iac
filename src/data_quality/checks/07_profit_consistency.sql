-- Business rule: profit must equal revenue - cost (no margin leakage in the ETL).
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated
WHERE ABS(profit - (revenue - cost)) > 0.01;
