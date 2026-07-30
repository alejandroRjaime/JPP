-- =============================================================================
-- The four benchmark queries. Portable SQL: runs unchanged on DuckDB and
-- ClickHouse once the tables are created (see duckdb.sql / clickhouse.sql).
--
-- Q1  4 dimensions, no predicates          — base case, high aggregation ratio
-- Q2  4 dimensions, 2 predicates           — selective dimension filters
-- Q3  4 dimensions, larger result          — reduced aggregation ratio
-- Q4  2 dimensions                         — dependence on N
--
-- Run each five times, discard the first, take the median of the rest.
-- Record the group count of every query: all five systems must agree on it
-- and on the aggregate sum before any timing is compared.
-- =============================================================================

-- Q1 ------------------------------------------------------------------------
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM   lineorder lo
JOIN   customer c ON lo.lo_custkey   = c.c_custkey
JOIN   supplier s ON lo.lo_suppkey   = s.s_suppkey
JOIN   part     p ON lo.lo_partkey   = p.p_partkey
JOIN   date_dim d ON lo.lo_orderdate = d.d_datekey
GROUP  BY d.d_year, c.c_nation
ORDER  BY d.d_year, c.c_nation;
-- Expected: 175 groups, sum 3000078095504.6

-- Q2 ------------------------------------------------------------------------
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM   lineorder lo
JOIN   customer c ON lo.lo_custkey   = c.c_custkey
JOIN   supplier s ON lo.lo_suppkey   = s.s_suppkey
JOIN   part     p ON lo.lo_partkey   = p.p_partkey
JOIN   date_dim d ON lo.lo_orderdate = d.d_datekey
WHERE  c.c_region = 'AMERICA'
  AND  s.s_region = 'AMERICA'
GROUP  BY d.d_year, c.c_nation
ORDER  BY d.d_year, c.c_nation;

-- Q3 ------------------------------------------------------------------------
SELECT d.d_year, c.c_nation, p.p_mfgr, SUM(lo.amount) AS total
FROM   lineorder lo
JOIN   customer c ON lo.lo_custkey   = c.c_custkey
JOIN   supplier s ON lo.lo_suppkey   = s.s_suppkey
JOIN   part     p ON lo.lo_partkey   = p.p_partkey
JOIN   date_dim d ON lo.lo_orderdate = d.d_datekey
WHERE  p.p_mfgr IN ('MFGR#1','MFGR#2')
GROUP  BY d.d_year, c.c_nation, p.p_mfgr
ORDER  BY d.d_year, c.c_nation, p.p_mfgr;

-- Q4 ------------------------------------------------------------------------
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM   lineorder lo
JOIN   customer c ON lo.lo_custkey   = c.c_custkey
JOIN   date_dim d ON lo.lo_orderdate = d.d_datekey
GROUP  BY d.d_year, c.c_nation
ORDER  BY d.d_year, c.c_nation;
