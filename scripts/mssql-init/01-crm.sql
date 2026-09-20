-- Seed for the demo SQL Server: a small "CRM" that DuckDB will join to Parquet.
IF DB_ID('demo') IS NULL CREATE DATABASE demo;
-- target database for the dbt project in section 09
IF DB_ID('refdata') IS NULL CREATE DATABASE refdata;
GO
USE refdata;
GO
IF SCHEMA_ID('calendar') IS NULL EXEC('CREATE SCHEMA calendar');
GO
USE demo;
GO
SET NOCOUNT ON;
DROP TABLE IF EXISTS dbo.customers;
DROP TABLE IF EXISTS dbo.segments;
CREATE TABLE dbo.segments (
    segment_id   tinyint      NOT NULL PRIMARY KEY,
    segment      nvarchar(20) NOT NULL,
    discount_pct decimal(4,1) NOT NULL
);
INSERT INTO dbo.segments VALUES (1, N'Consumer', 0), (2, N'Small business', 5), (3, N'Enterprise', 12.5);

-- 20,000 customers, ids matching customer_id in data/orders.parquet
WITH n AS (SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
           FROM sys.all_objects a CROSS JOIN sys.all_objects b)
SELECT CAST(i AS int)                                        AS customer_id,
       CONCAT(N'Customer ', i)                               AS name,
       CAST(CASE WHEN i % 10 = 0 THEN 3 WHEN i % 3 = 0 THEN 2 ELSE 1 END AS tinyint) AS segment_id,
       CAST(DATEADD(day, -(i % 2000), '2025-12-31') AS date) AS customer_since,
       CAST((i % 50) * 1000 AS money)                        AS credit_limit,
       CASE WHEN i % 97 = 0 THEN NULL ELSE CONCAT(N'c', i, N'@example.se') END AS email
INTO dbo.customers
FROM n;
ALTER TABLE dbo.customers ALTER COLUMN customer_id int NOT NULL;
ALTER TABLE dbo.customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);
GO
SET NOCOUNT ON;
SELECT CONCAT('seeded demo.dbo.customers: ', COUNT(*), ' rows') FROM dbo.customers;
GO
