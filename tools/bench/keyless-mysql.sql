-- Run this script on a disposable MySQL 8.4 database. The benchmark creates and removes a helper table and two procedures.
-- The measured predicates match the MySQL writer on these INT, VARCHAR and VARBINARY columns.
CREATE TABLE keyless_digits (i INT PRIMARY KEY);
INSERT INTO keyless_digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TEMPORARY TABLE keyless_probe (
    lookup_id INT, payload VARCHAR(20) COLLATE utf8mb4_0900_ai_ci, raw VARBINARY(32)
);
INSERT INTO keyless_probe
SELECT n, IF(n % 10 = 0, NULL, 'others'), UNHEX(MD5(n))
FROM (
    SELECT 1 + a.i + b.i*10 + c.i*100 + d.i*1000 + e.i*10000 AS n
    FROM keyless_digits a CROSS JOIN keyless_digits b CROSS JOIN keyless_digits c CROSS JOIN keyless_digits d CROSS JOIN keyless_digits e
) AS fixture
ORDER BY n;
DROP TABLE keyless_digits;
-- Add a duplicate and decoys with equivalent CHAR collation weight, NULL and a different byte value.
SET @probe_raw = UNHEX(MD5(100000));
INSERT INTO keyless_probe VALUES
    (100000, 'A', @probe_raw), (100000, 'a ', @probe_raw),
    (100000, 'a', 0xff), (100000, 'a', @probe_raw), (100000, 'a', @probe_raw);

PREPARE keyless_update FROM '
    UPDATE keyless_probe SET payload = ?
    WHERE lookup_id <=> ? AND payload <=> ? AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY ? AND raw <=> ? LIMIT 1';
PREPARE keyless_delete FROM '
    DELETE FROM keyless_probe
    WHERE lookup_id <=> ? AND payload <=> ? AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY ? AND raw <=> ? LIMIT 1';
SET @id = 100000, @before = 'a', @after = 'changed';

DELIMITER //
CREATE PROCEDURE bench_keyless()
BEGIN
    DECLARE run INT DEFAULT 0;
    DECLARE affected INT;
    DECLARE start_us DATETIME(6);
    WHILE run < 4 DO
        START TRANSACTION;
        SET start_us = SYSDATE(6);
        EXECUTE keyless_update USING @after, @id, @before, @before, @probe_raw;
        SET affected = ROW_COUNT();
        SELECT run, 'update' AS operation, TIMESTAMPDIFF(MICROSECOND, start_us, SYSDATE(6)) AS time_us, affected;
        IF affected <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'update did not affect one row'; END IF;
        SET start_us = SYSDATE(6);
        EXECUTE keyless_delete USING @id, @before, @before, @probe_raw;
        SET affected = ROW_COUNT();
        SELECT run, 'delete' AS operation, TIMESTAMPDIFF(MICROSECOND, start_us, SYSDATE(6)) AS time_us, affected;
        IF affected <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'delete did not affect one row'; END IF;
        IF run = 3 AND @keep_changes = 1 THEN COMMIT; ELSE ROLLBACK; END IF;
        SET run = run+1;
    END WHILE;
END//
DELIMITER ;

SELECT VERSION() AS mysql_version;
SELECT 'without_index' AS variant;
EXPLAIN FORMAT=JSON UPDATE keyless_probe SET payload = 'changed'
WHERE lookup_id <=> 100000 AND payload <=> 'a' AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY 'a' AND raw <=> @probe_raw LIMIT 1;
EXPLAIN FORMAT=JSON DELETE FROM keyless_probe
WHERE lookup_id <=> 100000 AND payload <=> 'a' AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY 'a' AND raw <=> @probe_raw LIMIT 1;
SET @keep_changes = 0;
SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next', 'Handler_read_rnd_next', 'Handler_read_key');
CALL bench_keyless();
SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next', 'Handler_read_rnd_next', 'Handler_read_key');

ALTER TABLE keyless_probe ADD INDEX keyless_lookup_id (lookup_id);
ANALYZE TABLE keyless_probe;
SELECT 'with_index' AS variant;
EXPLAIN FORMAT=JSON UPDATE keyless_probe SET payload = 'changed'
WHERE lookup_id <=> 100000 AND payload <=> 'a' AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY 'a' AND raw <=> @probe_raw LIMIT 1;
EXPLAIN FORMAT=JSON DELETE FROM keyless_probe
WHERE lookup_id <=> 100000 AND payload <=> 'a' AND BINARY CONVERT(payload USING utf8mb4) <=> BINARY 'a' AND raw <=> @probe_raw LIMIT 1;
SET @keep_changes = 1;
SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next', 'Handler_read_rnd_next', 'Handler_read_key');
CALL bench_keyless();
SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next', 'Handler_read_rnd_next', 'Handler_read_key');

-- Explicitly assert correct multiplicity of the changed row, the deleted duplicate and each decoy.
DELIMITER //
CREATE PROCEDURE check_keyless()
BEGIN
    DECLARE n, null_count, upper_count, space_count, raw_count, original_count, changed_count INT;
    SELECT COUNT(*),
           SUM(lookup_id = 100000 AND payload IS NULL),
           SUM(lookup_id = 100000 AND BINARY payload = 'A' AND raw = @probe_raw),
           SUM(lookup_id = 100000 AND BINARY payload = 'a ' AND raw = @probe_raw),
           SUM(lookup_id = 100000 AND BINARY payload = 'a' AND raw = 0xff),
           SUM(lookup_id = 100000 AND BINARY payload = 'a' AND raw = @probe_raw),
           SUM(lookup_id = 100000 AND BINARY payload = 'changed' AND raw = @probe_raw)
    INTO n, null_count, upper_count, space_count, raw_count, original_count, changed_count
    FROM keyless_probe;
    IF n <> 100004 OR null_count <> 1 OR upper_count <> 1 OR space_count <> 1 OR raw_count <> 1
       OR original_count <> 0 OR changed_count <> 1
    THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'keyless multiset differs';
    END IF;
END//
DELIMITER ;
CALL check_keyless();
SELECT 'PASS keyless multiset' AS result;
DROP PROCEDURE check_keyless;
DROP PROCEDURE bench_keyless;
