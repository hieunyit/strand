-- Audit retention without giving up append-only. audit_log still rejects UPDATE and TRUNCATE always, and DELETE
-- except inside strand_audit_archive(): that function first records a manifest row in audit_archives (row count,
-- id/time range, name and sha256 of the compressed JSONL export holding exactly those rows), then deletes the
-- rows older than the cutoff and adds an `audit.archive` entry to audit_log. The trigger lets a DELETE through
-- only for rows covered by a manifest row created in the SAME transaction (a nonce set by the function), never
-- for `audit.archive` entries: every removal of audit rows leaves a permanent, hash-referenced record.
-- `strand web db audit-archive` (CLI) and the optional automatic archive of the web call it after writing and
-- verifying the export file. Hardening: tables owned by an admin role, the web role granted INSERT/SELECT on
-- audit_log and EXECUTE on strand_audit_archive only (the function is SECURITY DEFINER).

CREATE OR REPLACE FUNCTION strand_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (% rejected)', TG_TABLE_NAME, TG_OP USING ERRCODE = 'insufficient_privilege';
END;
$$;

CREATE TABLE audit_archives (
    id           bigserial   PRIMARY KEY,
    created_at   timestamptz NOT NULL DEFAULT now(),
    created_txid bigint      NOT NULL DEFAULT txid_current(),
    nonce        text        NOT NULL UNIQUE,
    before_ts    timestamptz NOT NULL,
    max_id       bigint      NOT NULL,
    row_count    bigint      NOT NULL,
    min_id       bigint,
    min_ts       timestamptz,
    max_ts       timestamptz,
    file_name    text        NOT NULL,
    file_sha256  text        NOT NULL CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    detail       jsonb       NOT NULL DEFAULT '{}'
);
CREATE TRIGGER audit_archives_no_update_delete BEFORE UPDATE OR DELETE ON audit_archives
    FOR EACH ROW EXECUTE FUNCTION strand_audit_immutable();
CREATE TRIGGER audit_archives_no_truncate BEFORE TRUNCATE ON audit_archives
    FOR EACH STATEMENT EXECUTE FUNCTION strand_audit_immutable();

CREATE FUNCTION strand_audit_log_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_nonce text := current_setting('strand.audit_archive_nonce', true);
    v_before timestamptz;
    v_max_id bigint;
BEGIN
    IF TG_OP = 'DELETE' AND coalesce(v_nonce, '') <> '' AND OLD.action <> 'audit.archive' THEN
        SELECT before_ts, max_id INTO v_before, v_max_id FROM audit_archives
            WHERE nonce = v_nonce AND created_txid = txid_current();
        IF FOUND AND OLD.ts < v_before AND OLD.id <= v_max_id THEN
            RETURN OLD;
        END IF;
    END IF;
    RAISE EXCEPTION 'audit_log is append-only (% rejected)', TG_OP USING ERRCODE = 'insufficient_privilege';
END;
$$;
DROP TRIGGER audit_log_no_update_delete ON audit_log;
CREATE TRIGGER audit_log_no_update_delete BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION strand_audit_log_guard();

-- Deletes the audit rows with ts < p_before and id <= p_max_id (except audit.archive entries), only if there
-- are exactly p_count of them (= the rows in the verified export file), and records the archive. Returns the
-- number of rows deleted. Refuses a cutoff less than one day old.
CREATE FUNCTION strand_audit_archive(p_before timestamptz, p_max_id bigint, p_count bigint, p_file text, p_sha256 text, p_detail jsonb)
    RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_nonce text := md5(random()::text || clock_timestamp()::text || txid_current()::text);
    v_n bigint;
    v_min_id bigint;
    v_min_ts timestamptz;
    v_max_ts timestamptz;
BEGIN
    IF p_before > now() - interval '1 day' THEN
        RAISE EXCEPTION 'audit archive: the cutoff % is less than one day old', p_before;
    END IF;
    SELECT count(*), min(id), min(ts), max(ts) INTO v_n, v_min_id, v_min_ts, v_max_ts FROM audit_log
        WHERE ts < p_before AND id <= p_max_id AND action <> 'audit.archive';
    IF v_n <> p_count THEN
        RAISE EXCEPTION 'audit archive: % rows match in the database but the export file holds %; nothing deleted', v_n, p_count;
    END IF;
    IF v_n = 0 THEN
        RETURN 0;
    END IF;
    INSERT INTO audit_archives (nonce, before_ts, max_id, row_count, min_id, min_ts, max_ts, file_name, file_sha256, detail)
        VALUES (v_nonce, p_before, p_max_id, v_n, v_min_id, v_min_ts, v_max_ts, p_file, p_sha256, coalesce(p_detail, '{}'));
    PERFORM set_config('strand.audit_archive_nonce', v_nonce, true);
    DELETE FROM audit_log WHERE ts < p_before AND id <= p_max_id AND action <> 'audit.archive';
    GET DIAGNOSTICS v_n = ROW_COUNT;
    PERFORM set_config('strand.audit_archive_nonce', '', true);
    IF v_n <> p_count THEN
        RAISE EXCEPTION 'audit archive: deleted % rows, expected %; rolled back', v_n, p_count;
    END IF;
    INSERT INTO audit_log (ts, ip, action, pipeline, result, data) VALUES (now(), '', 'audit.archive', '', 'ok',
        jsonb_build_object('time', now(), 'ip', '', 'action', 'audit.archive', 'result', 'ok', 'detail',
            coalesce(p_detail, '{}') || jsonb_build_object('rows', v_n, 'min_id', v_min_id, 'max_id', p_max_id,
                'from', v_min_ts, 'to', v_max_ts, 'before', p_before, 'file', p_file, 'sha256', p_sha256)));
    RETURN v_n;
END;
$$;
