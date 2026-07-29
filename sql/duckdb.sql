-- DuckDB configuration used for the reported result (7.14 s).
--
-- NOTE: verify this against the configuration actually used for the paper.
-- If the reported DuckDB run read Parquet directly rather than loading into
-- native tables, either state that asymmetry with ClickHouse explicitly in the
-- paper or re-run both engines under the same storage arrangement.

SET threads TO 16;
SET memory_limit = '32GB';

CREATE TABLE customer  AS SELECT * FROM read_parquet('~/ssb_synth/customer/*.parquet');
CREATE TABLE supplier  AS SELECT * FROM read_parquet('~/ssb_synth/supplier/*.parquet');
CREATE TABLE part      AS SELECT * FROM read_parquet('~/ssb_synth/part/*.parquet');
CREATE TABLE date_dim  AS SELECT * FROM read_parquet('~/ssb_synth/date/*.parquet');
CREATE TABLE lineorder AS SELECT * FROM read_parquet('~/ssb_synth/lineorder/*.parquet');

.timer on

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
