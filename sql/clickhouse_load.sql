-- =============================================================================
-- ClickHouse: load the star schema into local MergeTree tables. Run once.
--
-- Columns arrive from Parquet as Nullable. MergeTree will not accept a nullable
-- sorting key, and a nullable column is also slower to scan, so every column is
-- converted with assumeNotNull: the generated data contains no nulls, and this
-- is the schema a practitioner would declare. Enabling allow_nullable_key
-- instead would work but would measure ClickHouse under a handicap the other
-- engines do not carry.
--
-- The source is the object store, where a copy of the dataset already lives;
-- the measurement is taken afterwards against these local tables.
-- Replace __ENDPOINT__, __KEY__ and __SECRET__ before running.
-- =============================================================================

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS lineorder;

CREATE TABLE customer ENGINE = MergeTree ORDER BY c_custkey AS
SELECT assumeNotNull(c_custkey) AS c_custkey,
       assumeNotNull(c_nation)  AS c_nation,
       assumeNotNull(c_region)  AS c_region
FROM s3('__ENDPOINT__/ssb-benchmark/sf600/customer/*.parquet', '__KEY__', '__SECRET__', 'Parquet');

CREATE TABLE supplier ENGINE = MergeTree ORDER BY s_suppkey AS
SELECT assumeNotNull(s_suppkey) AS s_suppkey,
       assumeNotNull(s_region)  AS s_region
FROM s3('__ENDPOINT__/ssb-benchmark/sf600/supplier/*.parquet', '__KEY__', '__SECRET__', 'Parquet');

CREATE TABLE part ENGINE = MergeTree ORDER BY p_partkey AS
SELECT assumeNotNull(p_partkey) AS p_partkey,
       assumeNotNull(p_mfgr)    AS p_mfgr
FROM s3('__ENDPOINT__/ssb-benchmark/sf600/part/*.parquet', '__KEY__', '__SECRET__', 'Parquet');

CREATE TABLE date_dim ENGINE = MergeTree ORDER BY d_datekey AS
SELECT assumeNotNull(d_datekey) AS d_datekey,
       assumeNotNull(d_year)    AS d_year
FROM s3('__ENDPOINT__/ssb-benchmark/sf600/date/*.parquet', '__KEY__', '__SECRET__', 'Parquet');

CREATE TABLE lineorder ENGINE = MergeTree ORDER BY lo_orderdate AS
SELECT assumeNotNull(lo_custkey)   AS lo_custkey,
       assumeNotNull(lo_suppkey)   AS lo_suppkey,
       assumeNotNull(lo_partkey)   AS lo_partkey,
       assumeNotNull(lo_orderdate) AS lo_orderdate,
       assumeNotNull(amount)       AS amount
FROM s3('__ENDPOINT__/ssb-benchmark/sf600/lineorder/*.parquet', '__KEY__', '__SECRET__', 'Parquet');

SELECT 'loaded' AS status, count() AS fact_rows FROM lineorder;
