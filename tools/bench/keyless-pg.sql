\set ON_ERROR_STOP on
-- Use psql -X -v rows=100000 (or 1000000) in a disposable session.
-- All tables and the index are temporary.
CREATE TEMP TABLE keyless_probe (lookup_id integer, payload text COLLATE "C", raw bytea);
INSERT INTO keyless_probe
SELECT i, CASE WHEN i % 10 = 0 THEN NULL ELSE md5(i::text) END, decode(md5(i::text), 'hex')
FROM generate_series(1, :rows) AS i;
INSERT INTO keyless_probe SELECT * FROM keyless_probe WHERE lookup_id = :rows;
ANALYZE keyless_probe;
SET work_mem = '4MB';

-- The target-like lookup retains the whole before-image and physical row identity.
PREPARE current_writer (integer, text, bytea) AS
DELETE FROM keyless_probe
WHERE (tableoid, ctid) = (
  SELECT tableoid, ctid FROM keyless_probe
  WHERE lookup_id IS NOT DISTINCT FROM $1 AND payload IS NOT DISTINCT FROM $2
    AND raw IS NOT DISTINCT FROM $3 LIMIT 1
);
PREPARE indexed_candidate (integer, text, bytea) AS
DELETE FROM keyless_probe
WHERE (tableoid, ctid) = (
  SELECT tableoid, ctid FROM keyless_probe
  WHERE lookup_id = $1 AND payload IS NOT DISTINCT FROM $2
    AND raw IS NOT DISTINCT FROM $3 LIMIT 1
);

-- Compare counts per canonical row, including identical duplicates, NULL and bytes.
-- EXCEPT ALL preserves multiplicities. work_mem bounds sort memory and spills to disk.
CREATE TEMP TABLE expected_rows AS TABLE keyless_probe;
DELETE FROM expected_rows WHERE ctid = (
  SELECT ctid FROM expected_rows WHERE lookup_id = :rows LIMIT 1
);
SELECT decode(md5((:rows)::text), 'hex') AS probe_bytes \gset

\echo current_writer_without_index
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;

CREATE INDEX ON keyless_probe (lookup_id);
ANALYZE keyless_probe;
\echo current_writer_with_index
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE current_writer(:rows, NULL, :'probe_bytes');
ROLLBACK;

\echo equality_candidate_with_index
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE indexed_candidate(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE indexed_candidate(:rows, NULL, :'probe_bytes');
ROLLBACK;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE indexed_candidate(:rows, NULL, :'probe_bytes');
ROLLBACK;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE indexed_candidate(:rows, NULL, :'probe_bytes');
DO $$
BEGIN
  IF EXISTS (
    (TABLE expected_rows EXCEPT ALL TABLE keyless_probe)
    UNION ALL
    (TABLE keyless_probe EXCEPT ALL TABLE expected_rows)
  ) THEN
    RAISE EXCEPTION 'Exact row multiset differs';
  END IF;
END
$$;
