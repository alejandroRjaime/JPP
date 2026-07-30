-- DuckDB: load the star schema into native tables. Run once.
SET threads TO 16;
SET memory_limit = '32GB';

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE customer  AS SELECT * FROM read_parquet('__DATA__/customer/*.parquet');
CREATE TABLE supplier  AS SELECT * FROM read_parquet('__DATA__/supplier/*.parquet');
CREATE TABLE part      AS SELECT * FROM read_parquet('__DATA__/part/*.parquet');
CREATE TABLE date_dim  AS SELECT * FROM read_parquet('__DATA__/date/*.parquet');
CREATE TABLE lineorder AS SELECT * FROM read_parquet('__DATA__/lineorder/*.parquet');

SELECT 'loaded' AS status, (SELECT count(*) FROM lineorder) AS fact_rows;
