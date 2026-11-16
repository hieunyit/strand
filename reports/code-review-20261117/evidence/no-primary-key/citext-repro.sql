-- Source and target setup: identical one-column keyless table and rows.
CREATE EXTENSION IF NOT EXISTS citext;
CREATE TABLE audit_citext(v citext COLLATE "C");
ALTER TABLE audit_citext REPLICA IDENTITY FULL;
INSERT INTO audit_citext VALUES ('alice'), ('ALICE');
-- Confirm the target guard sees a deterministic collation.
SELECT a.attname, co.collname, co.collisdeterministic
FROM pg_attribute a JOIN pg_collation co ON co.oid=a.attcollation
WHERE a.attrelid='audit_citext'::regclass AND a.attnum>0;
-- Source issues this, producing exactly the 'ALICE' before-image:
-- DELETE FROM audit_citext WHERE v::text = 'ALICE';
-- Generated target SQL for that before-image:
DELETE FROM audit_citext
WHERE ctid=(SELECT ctid FROM audit_citext WHERE v IS NOT DISTINCT FROM 'ALICE'::citext LIMIT 1)
RETURNING v::text;
-- Sequential heap scan normally deletes 'alice', leaving 'ALICE'.
SELECT v::text FROM audit_citext;
DROP TABLE audit_citext;
