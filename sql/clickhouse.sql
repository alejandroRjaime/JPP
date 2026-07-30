-- ClickHouse configuration used for the reported result (13.73 s).
--
-- Dimensions and fact are loaded into local MergeTree tables with an explicit
-- ORDER BY, and the query runs with the same thread budget as the other engines.
-- This is deliberate: reading Parquet through the s3() table function does not
-- benefit from the OS page cache the way Spark and DuckDB do, so measuring it
-- that way would compare network-bound reads against cached ones.
--
-- Replace <OBJECT_STORE> with the endpoint and credentials of your object store,
-- or point the paths at local Parquet files.

CREATE TABLE customer ENGINE = MergeTree ORDER BY c_custkey AS
  SELECT * FROM s3('<OBJECT_STORE>/ssb/customer/*.parquet', 'Parquet');

CREATE TABLE supplier ENGINE = MergeTree ORDER BY s_suppkey AS
  SELECT * FROM s3('<OBJECT_STORE>/ssb/supplier/*.parquet', 'Parquet');

CREATE TABLE part ENGINE = MergeTree ORDER BY p_partkey AS
  SELECT * FROM s3('<OBJECT_STORE>/ssb/part/*.parquet', 'Parquet');

CREATE TABLE date_dim ENGINE = MergeTree ORDER BY d_datekey AS
  SELECT * FROM s3('<OBJECT_STORE>/ssb/date/*.parquet', 'Parquet');

CREATE TABLE lineorder ENGINE = MergeTree ORDER BY lo_orderdate AS
  SELECT * FROM s3('<OBJECT_STORE>/ssb/lineorder/*.parquet', 'Parquet');

-- Match the thread budget of the other engines.
SET max_threads = 16;

-- Q1 of the query set. Q2-Q4 are in queries.sql.
SELECT d.d_year, c.c_nation, sum(lo.amount) AS total
FROM lineorder lo
JOIN customer  c ON lo.lo_custkey   = c.c_custkey
JOIN supplier  s ON lo.lo_suppkey   = s.s_suppkey
JOIN part      p ON lo.lo_partkey   = p.p_partkey
JOIN date_dim  d ON lo.lo_orderdate = d.d_datekey
GROUP BY d.d_year, c.c_nation
ORDER BY d.d_year, c.c_nation;
-- Expected: 175 groups.

-- Functional-equivalence check. Expected: 3000078095504.6
SELECT sum(lo.amount) AS verification_sum
FROM lineorder lo
JOIN customer  c ON lo.lo_custkey   = c.c_custkey
JOIN supplier  s ON lo.lo_suppkey   = s.s_suppkey
JOIN part      p ON lo.lo_partkey   = p.p_partkey
JOIN date_dim  d ON lo.lo_orderdate = d.d_datekey;
