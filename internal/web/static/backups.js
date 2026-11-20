(function () {
  'use strict';
  if (!window.strandApi || !document.querySelector('#backup-job-form, #restore-form, #repo-form, [data-backup-id]')) { return; }
  var jobID = '';
  var previewJob = '';
  // Default ports and password variable hints per SQL engine (prefilled when the port is empty or still
  // the other engine's default).
  var ENGINE_PORT = { mysql: 3306, postgres: 5432 };
  var ENGINE_PASSWORD_ENV = { mysql: 'MYSQL_BACKUP_PASSWORD', postgres: 'PGPASSWORD_BACKUP' };
  var PITR_LABEL = { mysql: 'Archive binlogs continuously', postgres: 'Archive WAL continuously', mongodb: 'Archive the oplog continuously' };
  var METHOD_HINT = {
    mysqldump: 'Logical dump streamed to the repository.',
    'mariadb-dump': 'Logical dump (MariaDB client) streamed to the repository.',
    mydumper: 'Parallel logical dump; faster for large databases.',
    pg_dump: 'Logical dump of the listed databases.',
    pg_basebackup: 'Physical copy of the whole cluster; restores into a new data directory.',
    mongodump: 'BSON dump of one database or the whole replica set.'
  };
  var DATABASES_HINT = {
    pg_dump: 'Comma separated; at least one database is required.',
    mongodump: 'One database, or empty for the whole replica set.'
  };
  // Frequency presets in minutes; anything else is a custom interval.
  var PRESETS = ['60', '360', '1440', '10080'];

  function form() { return document.getElementById('backup-job-form'); }
  function field(name) { return form().elements.namedItem(name); }
  function val(name) { var f = field(name); return f ? String(f.value || '').trim() : ''; }
  function num(name) { var n = Number(val(name)); return isFinite(n) ? n : 0; }
  function checked(name) { var f = field(name); return !!(f && f.checked); }
  function message() { return document.getElementById('backup-job-message'); }
  function showError(err) {
    var m = message();
    if (m) { m.textContent = err || 'Request failed'; m.classList.add('err'); }
  }
  function json(resp, fallback) {
    return resp.json().catch(function () { return {}; }).then(function (body) {
      if (!resp.ok) { throw new Error(body.error || fallback); }
      return body;
    });
  }
  function words(el, attr) { var v = el.getAttribute(attr); return v === null ? null : v.split(/\s+/).filter(Boolean); }

  // ---- generic conditional fields ----
  // data-show-if="name" shows the element when that control is checked / non-zero / non-empty;
  // data-show-if="name=value" when the control's value equals value (radio groups work too).
  function controlValue(f, name) {
    var c = f.elements.namedItem(name);
    if (!c) { return { value: '', on: false }; }
    if (c.type === 'checkbox') { return { value: c.checked ? 'on' : '', on: c.checked }; }
    var v = String(c.value || '').trim();
    return { value: v, on: v !== '' && v !== '0' };
  }
  function showIf(f, el) {
    var cond = el.getAttribute('data-show-if');
    var eq = cond.indexOf('=');
    if (eq < 0) { return controlValue(f, cond).on; }
    return controlValue(f, cond.slice(0, eq)).value === cond.slice(eq + 1);
  }
  function syncConditions(f) {
    if (!f) { return; }
    f.querySelectorAll('[data-show-if]').forEach(function (el) { el.hidden = !showIf(f, el); });
  }

  // ---- backup job form ----
  function engine() { var e = field('engine'); return (e && e.value) || 'mysql'; }
  function isCustom() { return engine() === 'custom'; }
  function method() { return isCustom() ? '' : val('method'); }
  function methodEngine(m) {
    var opt = form().querySelector('select[name=method] option[value="' + m + '"]');
    return opt ? opt.getAttribute('data-engine') : '';
  }
  // Restrict the method select to the chosen engine and keep a valid selection.
  function syncMethodOptions() {
    var sel = field('method'), e = engine(), first = null, keep = false;
    if (!sel) { return; }
    Array.prototype.forEach.call(sel.options, function (o) {
      var ok = o.getAttribute('data-engine') === e;
      o.hidden = !ok; o.disabled = !ok;
      if (ok && !first) { first = o; }
      if (ok && o.value === sel.value) { keep = true; }
    });
    if (!keep && first) { sel.value = first.value; }
  }
  function syncEngine() {
    var f = form();
    if (!f) { return; }
    syncMethodOptions();
    var e = engine(), m = method();
    f.querySelectorAll('[data-engines]').forEach(function (el) {
      var hide = words(el, 'data-engines').indexOf(e) < 0;
      var ms = words(el, 'data-methods');
      if (!hide && ms && ms.indexOf(m) < 0) { hide = true; }
      el.hidden = hide;
    });
    if (ENGINE_PORT[e]) {
      var port = field('conn_port'), cur = Number(port.value) || 0;
      if (!cur || cur === ENGINE_PORT.mysql || cur === ENGINE_PORT.postgres) { port.value = ENGINE_PORT[e]; }
      field('conn_password_env').placeholder = ENGINE_PASSWORD_ENV[e];
    }
    if (e === 'custom') { field('repository').value = ''; }
    var pitr = document.getElementById('backup-pitr-label');
    if (pitr && PITR_LABEL[e]) { pitr.textContent = PITR_LABEL[e]; }
    var mh = document.getElementById('backup-method-hint');
    if (mh) { mh.textContent = METHOD_HINT[m] || ''; }
    var dh = document.getElementById('backup-databases-hint');
    if (dh) { dh.textContent = DATABASES_HINT[m] || 'Comma separated; empty backs up every database.'; }
    syncPitr();
    syncConditions(f);
  }
  function pitrSupport() {
    var e = engine(), m = method();
    if (e === 'custom') {
      return { ok: false, reason: 'Custom scripts do not support point-in-time recovery' };
    }
    if (m === 'mariadb-dump') {
      return { ok: false, reason: 'MariaDB does not support binlog archiving' };
    }
    if (m === 'pg_dump') {
      return { ok: false, reason: 'PITR needs pg_basebackup as the base backup' };
    }
    if (m === 'mydumper') {
      return { ok: false, reason: 'mydumper does not support binlog archiving' };
    }
    if (m === 'mysqldump') {
      var dbs = list(val('conn_databases'));
      if (dbs.length !== 1) {
        return { ok: false, reason: 'PITR requires exactly one database' };
      }
    }
    if (m === 'pg_basebackup' || m === 'mongodump' || m === 'mysqldump') {
      return { ok: true, reason: '' };
    }
    return { ok: false, reason: 'Method does not support point-in-time recovery' };
  }
  function syncPitr() {
    var pitrBox = field('log_archive');
    if (!pitrBox) { return; }
    var sup = pitrSupport();
    var hint = document.getElementById('backup-pitr-hint');
    var reason = document.getElementById('backup-pitr-reason');
    if (!sup.ok) {
      pitrBox.disabled = true;
      pitrBox.checked = false;
      if (reason) {
        reason.textContent = sup.reason;
        reason.hidden = false;
      }
      if (hint) { hint.hidden = true; }
    } else {
      pitrBox.disabled = false;
      if (reason) {
        reason.textContent = '';
        reason.hidden = true;
      }
      if (hint) { hint.hidden = false; }
    }
  }
  function list(s) { return s.split(',').map(function (x) { return x.trim(); }).filter(Boolean); }
  function conn(prefix) {
    return { host: val(prefix + '_host'), port: num(prefix + '_port'), user: val(prefix + '_user'),
      password_env: val(prefix + '_password_env'), uri_env: '', databases: [] };
  }
  function intervalMinutes() { return val('frequency') === 'custom' ? num('interval_minutes') : Number(val('frequency')); }
  // readJob sends only the fields of the chosen engine/method; hidden fields go out empty so a payload
  // never mixes engines (e.g. a mysqldump job never carries conn.uri_env).
  function readJob() {
    var e = engine(), m = method(), custom = e === 'custom';
    var repo = custom ? '' : val('repository');
    var job = {
      name: val('name'), method: m,
      every_seconds: intervalMinutes() * 60, bucket: repo ? '' : val('bucket'), prefix: val('prefix'),
      enabled: checked('enabled'), repository: repo, replicate_to: val('replicate_to'),
      compress: checked('compress'), encrypt: checked('encrypt'), bandwidth_kbps: num('bandwidth_kbps'),
      retry_max: num('retry_max'), retry_backoff_seconds: num('retry_max') > 0 ? num('retry_backoff_seconds') : 0,
      verify_every_seconds: num('verify_hours') * 3600, verify_sample_percent: num('verify_sample_percent'),
      retention: { keep_last: num('keep_last'), keep_daily: num('keep_daily'), keep_weekly: num('keep_weekly'),
        keep_monthly: num('keep_monthly'), keep_yearly: num('keep_yearly'), keep_within_days: num('keep_within_days'),
        max_age_days: num('max_age_days'), lock_days: num('lock_days') },
      tunnel_ssh: '', tunnel_user: '', tunnel_remote: '', tunnel_identity_file: '', tunnel_known_hosts: '',
      command: '', artifact_template: '', manifest_template: '', cleanup_local: false,
      conn: {}, scratch: {}
    };
    if (custom) {
      job.command = val('command'); job.artifact_template = val('artifact_template');
      job.manifest_template = val('manifest_template'); job.cleanup_local = checked('cleanup_local');
      job.tunnel_ssh = val('tunnel_ssh'); job.tunnel_user = val('tunnel_user'); job.tunnel_remote = val('tunnel_remote');
      job.tunnel_identity_file = val('tunnel_identity_file'); job.tunnel_known_hosts = val('tunnel_known_hosts');
      return job;
    }
    job.source = val('source') || val('name');
    if (e === 'mongodb') {
      job.conn = { host: '', port: 0, user: '', password_env: '', uri_env: val('conn_uri_env'), databases: [] };
    } else {
      job.conn = conn('conn');
    }
    if (m !== 'pg_basebackup') { job.conn.databases = list(val('conn_databases')); }
    var pitrField = field('log_archive');
    job.log_archive = !!(pitrField && !pitrField.disabled && pitrField.checked);
    job.log_flush_seconds = job.log_archive ? num('log_flush_seconds') : 0;
    job.test_restore = (m === 'mysqldump' || m === 'mariadb-dump' || m === 'pg_dump') && checked('test_restore');
    if (job.test_restore) { job.scratch = conn('scratch'); }
    return job;
  }
  function setVal(name, v) { var f = field(name); if (f) { f.value = (v === undefined || v === null) ? '' : v; } }
  function setRadio(f, name, v) {
    var r = f.querySelector('input[type=radio][name="' + name + '"][value="' + v + '"]');
    if (r) { r.checked = true; }
  }
  function drawer() { return document.getElementById('backup-job-drawer'); }
  function openDrawer(title, save) {
    var d = drawer();
    document.getElementById('backup-job-form-title').textContent = title;
    document.getElementById('backup-job-save').textContent = save;
    d.hidden = false;
    // An open drawer counts as editing: the auto-refresh must not replace it.
    form().setAttribute('data-dirty', '1');
    if (d.scrollIntoView) { d.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
  }
  function fill(job) {
    var f = form(), c = job.conn || {}, r = job.retention || {}, s = job.scratch || {};
    f.reset();
    var custom = !job.method || job.method === 'custom';
    var e = custom ? 'custom' : (methodEngine(job.method) || 'mysql');
    setRadio(f, 'engine', e);
    syncMethodOptions();
    ['name', 'command', 'artifact_template', 'manifest_template', 'bucket', 'prefix', 'tunnel_ssh', 'tunnel_user',
      'tunnel_remote', 'tunnel_identity_file', 'tunnel_known_hosts', 'source', 'repository', 'replicate_to'].forEach(function (n) { setVal(n, job[n]); });
    if (!custom) { setVal('method', job.method); }
    var minutes = Math.max(1, Math.floor((job.every_seconds || 60) / 60));
    setVal('frequency', PRESETS.indexOf(String(minutes)) >= 0 ? String(minutes) : 'custom');
    setVal('interval_minutes', minutes);
    setVal('conn_host', c.host); setVal('conn_port', c.port || ''); setVal('conn_user', c.user);
    setVal('conn_password_env', c.password_env); setVal('conn_uri_env', c.uri_env);
    setVal('conn_databases', (c.databases || []).join(', '));
    setVal('scratch_host', s.host); setVal('scratch_port', s.port || ''); setVal('scratch_user', s.user); setVal('scratch_password_env', s.password_env);
    ['keep_last', 'keep_daily', 'keep_weekly', 'keep_monthly', 'keep_yearly', 'keep_within_days', 'max_age_days', 'lock_days'].forEach(function (n) { setVal(n, r[n] || 0); });
    setVal('log_flush_seconds', job.log_flush_seconds || 0);
    setVal('bandwidth_kbps', job.bandwidth_kbps || 0); setVal('retry_max', job.retry_max || 0);
    setVal('retry_backoff_seconds', job.retry_backoff_seconds || 300);
    setVal('verify_hours', Math.round((job.verify_every_seconds || 0) / 3600)); setVal('verify_sample_percent', job.verify_sample_percent || 100);
    field('enabled').checked = !!job.enabled; field('cleanup_local').checked = !!job.cleanup_local;
    field('compress').checked = !!job.compress; field('encrypt').checked = !!job.encrypt;
    field('log_archive').checked = !!job.log_archive; field('test_restore').checked = !!job.test_restore;
    jobID = job.id;
    syncEngine();
    var advanced = !!(job.log_archive || job.log_flush_seconds || job.bandwidth_kbps || job.replicate_to ||
      r.keep_weekly || r.keep_monthly || r.keep_yearly || r.keep_within_days || r.max_age_days || r.lock_days || r.keep_hourly ||
      job.retry_max || job.verify_every_seconds || (job.verify_sample_percent && job.verify_sample_percent !== 100) ||
      job.test_restore || job.tunnel_ssh || job.tunnel_user || job.tunnel_remote || job.tunnel_identity_file || job.tunnel_known_hosts);
    document.getElementById('backup-job-advanced').open = advanced;
    clearMessage();
    openDrawer('Edit backup job', 'Save changes');
  }
  function clearMessage() { var m = message(); if (m) { m.textContent = ''; m.classList.remove('err'); } }
  function reset() {
    var f = form();
    if (!f) { return; }
    f.reset(); jobID = '';
    document.getElementById('backup-job-advanced').open = false;
    syncEngine();
    clearMessage();
    f.removeAttribute('data-dirty');
    drawer().hidden = true;
  }
  function closeMenus(except) {
    document.querySelectorAll('details.rowmenu[open]').forEach(function (d) { if (d !== except) { d.open = false; } });
  }
  function cell(text, cls) { var td = document.createElement('td'); td.textContent = text; if (cls) { td.className = cls; } return td; }
  function showPreview(id, body) {
    previewJob = id;
    var box = document.getElementById('retention-preview');
    var rows = document.getElementById('retention-preview-rows');
    rows.textContent = '';
    (body.decisions || []).forEach(function (d) {
      var tr = document.createElement('tr');
      tr.appendChild(cell(d.id, 'mono small'));
      tr.appendChild(cell(d.kind));
      tr.appendChild(cell(d.created_at, 'nowrap'));
      var verdict = document.createElement('td');
      var b = document.createElement('span');
      b.className = 'badge ' + (d.keep ? (d.blocked ? 'st-paused' : 'st-synced') : 'st-failed');
      b.textContent = d.keep ? (d.blocked ? 'kept (blocked)' : 'keep') : 'expire';
      verdict.appendChild(b);
      tr.appendChild(verdict);
      tr.appendChild(cell((d.reasons || []).join('; '), 'small'));
      rows.appendChild(tr);
    });
    document.getElementById('retention-preview-job').textContent = id;
    document.getElementById('retention-preview-summary').textContent = body.keep + ' kept, ' + body.expire + ' would be expired (objects deleted from S3).';
    box.hidden = false;
    if (form()) { form().setAttribute('data-dirty', '1'); }
    if (box.scrollIntoView) { box.scrollIntoView({ behavior: 'smooth', block: 'nearest' }); }
  }
  function go(resp, fallback) {
    return json(resp, fallback).then(function (body) {
      if (body.process_id) { location.href = '/jobs/' + encodeURIComponent(body.process_id); } else { location.reload(); }
    });
  }

  // ---- repository drawer ----
  function repoForm() { return document.getElementById('repo-form'); }
  function setRepoDrawer(open) {
    var d = document.getElementById('repo-drawer'), f = repoForm();
    if (!d || !f) { return; }
    if (open) {
      d.hidden = false; f.setAttribute('data-dirty', '1');
      if (d.scrollIntoView) { d.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
    } else {
      f.reset(); syncConditions(f); f.removeAttribute('data-dirty'); d.hidden = true;
      document.getElementById('repo-message').textContent = '';
    }
  }

  // ---- restore form ----
  function restoreForm() { return document.getElementById('restore-form'); }

  document.addEventListener('change', function (event) {
    var t = event.target;
    if (!t.form) { return; }
    if (t.form.id === 'backup-job-form') {
      if (t.name === 'engine' || t.name === 'method' || t.name === 'conn_databases') { syncEngine(); } else { syncConditions(t.form); }
    } else if (t.form.id === 'restore-form' || t.form.id === 'repo-form') {
      syncConditions(t.form);
    }
  });
  document.addEventListener('click', function (event) {
    var t = event.target;
    if (t.closest('#backup-job-new')) {
      reset();
      openDrawer('New backup job', 'Create job');
      var n = field('name'); if (n && n.focus) { n.focus(); }
      return;
    }
    if (t.closest('#backup-job-cancel') || t.closest('#backup-job-close')) { reset(); return; }
    if (t.closest('#repo-new')) { setRepoDrawer(true); return; }
    if (t.closest('#repo-cancel') || t.closest('#repo-close')) { setRepoDrawer(false); return; }
    if (t.closest('#retention-close')) {
      document.getElementById('retention-preview').hidden = true; previewJob = '';
      if (form() && drawer().hidden) { form().removeAttribute('data-dirty'); }
      return;
    }
    if (t.closest('#retention-apply')) {
      if (!previewJob || !window.confirm('Delete the backups marked "expire" from S3 now? This cannot be undone.')) { return; }
      window.strandApi('POST', '/api/v1/backup-jobs/' + encodeURIComponent(previewJob) + '/prune').then(function (resp) { return go(resp, 'Could not start retention'); })
        .catch(function (err) { window.alert(err.message); });
      return;
    }
    var repoBtn = t.closest('[data-repo-action]');
    if (repoBtn) {
      var repoID = repoBtn.closest('[data-repo-id]').getAttribute('data-repo-id');
      if (!window.confirm('Delete repository ' + repoID + '? Only possible when it holds no backups.')) { return; }
      window.strandApi('DELETE', '/api/v1/backup-repositories/' + encodeURIComponent(repoID)).then(function (resp) {
        if (!resp.ok) { return json(resp, 'Could not delete repository'); }
        location.reload();
      }).catch(function (err) { window.alert(err.message); });
      return;
    }
    var bBtn = t.closest('[data-backup-action]');
    if (bBtn) {
      closeMenus();
      var bRow = bBtn.closest('[data-backup-id]');
      var bid = bRow.getAttribute('data-backup-id');
      var act = bBtn.getAttribute('data-backup-action');
      var base = '/api/v1/backups/' + encodeURIComponent(bid);
      if (act === 'restore') {
        var rf = restoreForm();
        if (!rf) { location.href = '/backups'; return; }
        var rTab = document.querySelector('[data-tab-target="restore"]');
        if (rTab) { rTab.click(); }
        setRadio(rf, 'restore_from', 'backup');
        if (bRow.getAttribute('data-backup-tool') === 'pg_basebackup') { setRadio(rf, 'restore_into', 'dir'); }
        rf.elements.namedItem('backup_id').value = bid; rf.elements.namedItem('source').value = '';
        syncConditions(rf);
        rf.setAttribute('data-dirty', '1');
        if (rf.scrollIntoView) { rf.scrollIntoView({ behavior: 'smooth', block: 'start' }); }
        return;
      }
      var req;
      if (act === 'verify') {
        if (!window.confirm('Re-read and verify backup ' + bid + '?')) { return; }
        req = window.strandApi('POST', base + '/verify');
      } else if (act === 'hold') {
        var hold = bBtn.getAttribute('data-hold') === '1';
        if (!window.confirm((hold ? 'Put ' : 'Release ') + bid + (hold ? ' on legal hold? Retention will never delete it.' : ' from legal hold?'))) { return; }
        req = window.strandApi('POST', base + '/legal-hold', { hold: hold });
      } else if (act === 'copy') {
        var repo = window.prompt('Copy ' + bid + ' to repository (id):');
        if (!repo) { return; }
        req = window.strandApi('POST', base + '/copy', { repository: repo.trim() });
      }
      if (req) { req.then(function (resp) { return go(resp, 'Request failed'); }).catch(function (err) { window.alert(err.message); }); }
      return;
    }
    var button = t.closest('[data-backup-job-action]');
    if (!button) { return; }
    closeMenus();
    var row = button.closest('[data-backup-job-id]');
    var id = row && row.getAttribute('data-backup-job-id');
    if (!id) { return; }
    var action = button.getAttribute('data-backup-job-action');
    var url = '/api/v1/backup-jobs/' + encodeURIComponent(id);
    if (action === 'edit') {
      if (!form()) { return; }
      window.strandApi('GET', url).then(function (resp) { return json(resp, 'Could not load job'); })
        .then(function (body) { fill(body.job); }).catch(function (err) { window.alert(err.message); });
      return;
    }
    if (action === 'preview') {
      window.strandApi('POST', url + '/retention-preview').then(function (resp) { return json(resp, 'Could not compute retention'); })
        .then(function (body) { showPreview(id, body); }).catch(function (err) { window.alert(err.message); });
      return;
    }
    var prompts = { start: 'Start this scheduled backup job? The first backup runs immediately.', stop: 'Pause this backup job?',
      delete: 'Delete this backup job definition? Existing backups will remain stored.',
      run: 'Run one backup now? It waits for any run of this job that is already in progress.',
      verify: 'Verify every backup of this job now (re-read objects, chains, log continuity)?' };
    if (!prompts[action] || !window.confirm(prompts[action])) { return; }
    var method = action === 'delete' ? 'DELETE' : 'POST';
    window.strandApi(method, url + (action === 'delete' ? '' : '/' + action)).then(function (resp) {
      if (action === 'verify') { return go(resp, 'Request failed'); }
      if (!resp.ok) { return json(resp, 'Request failed'); }
      location.reload();
    }).catch(function (err) { window.alert(err.message); });
  });
  function isoUTC(local) {
    if (!local) { return ''; }
    var s = local.length === 16 ? local + ':00' : local;
    return s + 'Z';
  }
  function status(el, text) { if (el) { el.textContent = text; el.classList.toggle('err', !!text); } }
  document.addEventListener('submit', function (event) {
    if (event.target.matches('#backup-job-form')) {
      event.preventDefault(); clearMessage();
      if (!val('name')) { showError('Give the job a name.'); return; }
      var minutes = intervalMinutes();
      if (!(minutes >= 1)) { showError('Choose how often the job runs.'); return; }
      var payload = readJob();
      var verb = jobID ? 'PUT' : 'POST';
      var url = '/api/v1/backup-jobs' + (jobID ? '/' + encodeURIComponent(jobID) : '');
      window.strandApi(verb, url, payload).then(function (resp) { return json(resp, 'Could not save backup job'); })
        .then(function () { form().removeAttribute('data-dirty'); location.reload(); }).catch(function (err) { showError(err.message); });
      return;
    }
    if (event.target.matches('#restore-form')) {
      event.preventDefault();
      var f = event.target.elements;
      var get = function (n) { return String(f.namedItem(n).value || '').trim(); };
      var out = document.getElementById('restore-message');
      var fromBackup = get('restore_from') !== 'pitr', intoDir = get('restore_into') === 'dir';
      var body = { backup_id: fromBackup ? get('backup_id') : '', source: fromBackup ? '' : get('source'), target_time: isoUTC(get('target_time')),
        target: { host: get('target_host'), port: Number(get('target_port')) || 0, user: get('target_user'), password_env: get('target_password_env') },
        target_database: intoDir ? '' : get('target_database'), target_dir: intoDir ? get('target_dir') : '', confirm: get('confirm') };
      if (fromBackup && !body.backup_id) { status(out, 'Enter the backup ID to restore.'); return; }
      if (!fromBackup && (!body.source || !body.target_time)) { status(out, 'A point-in-time restore needs a source and a time.'); return; }
      if (!(body.target_database + body.target_dir)) { status(out, intoDir ? 'Enter the new data directory.' : 'Enter the new database name.'); return; }
      if (body.confirm !== body.target_database + body.target_dir) { status(out, 'Type the target database or directory exactly to confirm.'); return; }
      status(out, '');
      if (!window.confirm('Start the restore into ' + body.confirm + '? The source is not modified.')) { return; }
      window.strandApi('POST', '/api/v1/backup-restores', body).then(function (resp) { return go(resp, 'Could not start restore'); })
        .catch(function (err) { status(out, err.message); });
      return;
    }
    if (event.target.matches('#repo-form')) {
      event.preventDefault();
      var rf = event.target.elements;
      var g = function (n) { return String(rf.namedItem(n).value || '').trim(); };
      var lockMode = g('lock_mode');
      var repo = { name: g('name'), env_prefix: g('env_prefix'), bucket: g('bucket'), prefix: g('prefix'), lock_mode: lockMode,
        lock_days: lockMode ? (Number(g('lock_days')) || 0) : 0, retention: { keep_daily: Number(g('keep_daily')) || 0, max_age_days: Number(g('max_age_days')) || 0 } };
      window.strandApi('POST', '/api/v1/backup-repositories', repo).then(function (resp) { return json(resp, 'Could not save repository'); })
        .then(function () { repoForm().removeAttribute('data-dirty'); location.reload(); })
        .catch(function (err) { status(document.getElementById('repo-message'), err.message); });
    }
  });
  document.addEventListener('input', function (event) {
    var f = event.target.form;
    if (f && (f.id === 'restore-form' || f.id === 'repo-form' || f.id === 'backup-job-form')) {
      f.setAttribute('data-dirty', '1');
      if (event.target.type === 'number') { syncConditions(f); }
      if (event.target.name === 'conn_databases') { syncPitr(); }
    }
  });
  if (form()) { syncEngine(); form().removeAttribute('data-dirty'); }
  syncConditions(restoreForm());
  syncConditions(repoForm());
}());
