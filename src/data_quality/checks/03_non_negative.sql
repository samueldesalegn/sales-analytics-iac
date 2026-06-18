-- Validity: revenue and units must never be negative.
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated
WHERE revenue < 0 OR units < 0;
