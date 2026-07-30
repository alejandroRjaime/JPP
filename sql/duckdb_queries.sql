-- DuckDB: the benchmark query set. Run against an already-loaded database.
SET threads TO 16;
SET memory_limit = '32GB';
.timer on

-- Q1
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM lineorder lo
JOIN customer c ON lo.lo_custkey = c.c_custkey
JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
JOIN part     p ON lo.lo_partkey = p.p_partkey
JOIN date_dim d ON lo.lo_orderdate = d.d_datekey
GROUP BY d.d_year, c.c_nation;

-- Q2
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM lineorder lo
JOIN customer c ON lo.lo_custkey = c.c_custkey
JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
JOIN part     p ON lo.lo_partkey = p.p_partkey
JOIN date_dim d ON lo.lo_orderdate = d.d_datekey
WHERE c.c_region = 'AMERICA' AND s.s_region = 'AMERICA'
GROUP BY d.d_year, c.c_nation;

-- Q3
SELECT d.d_year, c.c_nation, p.p_mfgr, SUM(lo.amount) AS total
FROM lineorder lo
JOIN customer c ON lo.lo_custkey = c.c_custkey
JOIN supplier s ON lo.lo_suppkey = s.s_suppkey
JOIN part     p ON lo.lo_partkey = p.p_partkey
JOIN date_dim d ON lo.lo_orderdate = d.d_datekey
WHERE p.p_mfgr IN ('MFGR#1','MFGR#2')
GROUP BY d.d_year, c.c_nation, p.p_mfgr;

-- Q4
SELECT d.d_year, c.c_nation, SUM(lo.amount) AS total
FROM lineorder lo
JOIN customer c ON lo.lo_custkey = c.c_custkey
JOIN date_dim d ON lo.lo_orderdate = d.d_datekey
GROUP BY d.d_year, c.c_nation;
