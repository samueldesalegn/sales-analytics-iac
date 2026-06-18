-- Validity: units, revenue, and cost must never be negative.
SELECT COUNT(*) = 0                  AS passed,
       CAST(COUNT(*) AS varchar)     AS detail
FROM sales_curated
WHERE units < 0 OR revenue < 0 OR cost < 0;
