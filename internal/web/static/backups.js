(function () {
  'use strict';
  if (!window.strandApi || !document.getElementById('backup-job-form')) { return; }
  var jobID = '';
  var previewJob = '';
  function form() { return document.getElementById('backup-job-form'); }
  function field(name) { return form().elements.namedItem(name); }
  function val(name) { var f = field(name); return f ? String(f.value || '').trim() : ''; }
  function num(name) { var n = Number(val(name)); return isFinite(n) ? n : 0; }
  function checked(name) { var f = field(name); return !!(f && f.checked); }
  function message() { return document.getElementById('backup-job-message'); }
  function showError(err) { if (message()) { message().textContent = err || 'Request failed'; } }
  function json(resp, fallback) {
    return resp.json().catch(function () { return {}; }).then(function (body) {
      if (!resp.ok) { throw new Error(body.error || fallback); }
      return body;
    });
  }
  function isCustom() { return !val('method') || val('method') === 'custom'; }
  function syncMethod() {
    var custom = isCustom();
    form().querySelectorAll('[data-method-group]').forEach(function (fs) {
      fs.hidden = (fs.getAttribute('data-method-group') === 'custom') !== custom;
    });
  }
  function list(s) { return s.split(',').map(function (x) { return x.trim(); }).filter(Boolean); }
  function conn(prefix) {
    return { host: val(prefix + '_host'), port: num(prefix + '_port'), user: val(prefix + '_user'),
      password_env: val(prefix + '_password_env') };
  }
  function readJob() {
    var job = {
      name: val('name'), method: val('method') === 'custom' ? '' : val('method'),
      every_seconds: num('interval_minutes') * 60, bucket: val('bucket'), prefix: val('prefix'),
      enabled: checked('enabled'), repository: val('repository'), replicate_to: val('replicate_to'),
      compress: checked('compress'), encrypt: checked('encrypt'), bandwidth_kbps: num('bandwidth_kbps'),
      retry_max: num('retry_max'), retry_backoff_seconds: num('retry_max') > 0 ? num('retry_backoff_seconds') : 0,
      verify_every_seconds: num('verify_hours') * 3600, verify_sample_percent: num('verify_sample_percent'),
      retention: { keep_last: num('keep_last'), keep_daily: num('keep_daily'), keep_weekly: num('keep_weekly'),
        keep_monthly: num('keep_monthly'), keep_yearly: num('keep_yearly'), keep_within_days: num('keep_within_days'),
        max_age_days: num('max_age_days'), lock_days: num('lock_days') },
      tunnel_ssh: val('tunnel_ssh'), tunnel_user: val('tunnel_user'), tunnel_remote: val('tunnel_remote'),
      tunnel_identity_file: val('tunnel_identity_file'), tunnel_known_hosts: val('tunnel_known_hosts'),
      command: '', artifact_template: '', manifest_template: '', cleanup_local: false,
      conn: {}, scratch: {}
    };
    if (isCustom()) {
      job.command = val('command'); job.artifact_template = val('artifact_template');
      job.manifest_template = val('manifest_template'); job.cleanup_local = checked('cleanup_local');
    } else {
      job.source = val('source');
      job.conn = conn('conn');
      job.conn.uri_env = val('conn_uri_env'); job.conn.databases = list(val('conn_databases'));
      job.log_archive = checked('log_archive');
      job.log_flush_seconds = num('log_flush_seconds');
      job.test_restore = checked('test_restore');
      if (job.test_restore) { job.scratch = conn('scratch'); }
    }
    return job;
  }
  function setVal(name, v) { var f = field(name); if (f) { f.value = (v === undefined || v === null) ? '' : v; } }
  function fill(job) {
    var c = job.conn || {}, r = job.retention || {}, s = job.scratch || {};
    ['name', 'command', 'artifact_template', 'manifest_template', 'bucket', 'prefix', 'tunnel_ssh', 'tunnel_user',
      'tunnel_remote', 'tunnel_identity_file', 'tunnel_known_hosts', 'source', 'repository', 'replicate_to'].forEach(function (n) { setVal(n, job[n]); });
    setVal('method', job.method || 'custom');
    setVal('interval_minutes', Math.max(1, Math.floor(job.every_seconds / 60)));
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
    syncMethod();
    document.getElementById('backup-job-form-title').textContent = 'Edit backup job';
    document.getElementById('backup-job-save').textContent = 'Save changes';
    document.getElementById('backup-job-cancel').hidden = false;
    form().setAttribute('data-dirty', '1');
    if (form().scrollIntoView) { form().scrollIntoView({ behavior: 'smooth', block: 'start' }); }
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
    form().setAttribute('data-dirty', '1');
  }
  function reset() {
    form().reset(); jobID = '';
    syncMethod();
    document.getElementById('backup-job-form-title').textContent = 'Create backup job';
    document.getElementById('backup-job-save').textContent = 'Create job';
    document.getElementById('backup-job-cancel').hidden = true; message().textContent = ''; form().removeAttribute('data-dirty');
  }
  function go(resp, fallback) {
    return json(resp, fallback).then(function (body) {
      if (body.process_id) { location.href = '/jobs/' + encodeURIComponent(body.process_id); } else { location.reload(); }
    });
  }
  document.addEventListener('change', function (event) {
    if (event.target.matches && event.target.matches('#backup-job-form [name=method]')) { syncMethod(); }
  });
  document.addEventListener('click', function (event) {
    if (event.target.closest('#backup-job-cancel')) { reset(); return; }
    if (event.target.closest('#retention-close')) {
      document.getElementById('retention-preview').hidden = true; previewJob = ''; form().removeAttribute('data-dirty'); return;
    }
    if (event.target.closest('#retention-apply')) {
      if (!previewJob || !window.confirm('Delete the backups marked "expire" from S3 now? This cannot be undone.')) { return; }
      window.strandApi('POST', '/api/v1/backup-jobs/' + encodeURIComponent(previewJob) + '/prune').then(function (resp) { return go(resp, 'Could not start retention'); })
        .catch(function (err) { window.alert(err.message); });
      return;
    }
    var repoBtn = event.target.closest('[data-repo-action]');
    if (repoBtn) {
      var repoID = repoBtn.closest('[data-repo-id]').getAttribute('data-repo-id');
      if (!window.confirm('Delete repository ' + repoID + '? Only possible when it holds no backups.')) { return; }
      window.strandApi('DELETE', '/api/v1/backup-repositories/' + encodeURIComponent(repoID)).then(function (resp) {
        if (!resp.ok) { return json(resp, 'Could not delete repository'); }
        location.reload();
      }).catch(function (err) { window.alert(err.message); });
      return;
    }
    var bBtn = event.target.closest('[data-backup-action]');
    if (bBtn) {
      var bid = bBtn.closest('[data-backup-id]').getAttribute('data-backup-id');
      var act = bBtn.getAttribute('data-backup-action');
      var base = '/api/v1/backups/' + encodeURIComponent(bid);
      if (act === 'restore') {
        var rf = document.getElementById('restore-form');
        rf.elements.namedItem('backup_id').value = bid; rf.elements.namedItem('source').value = '';
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
    var button = event.target.closest('[data-backup-job-action]');
    if (!button) { return; }
    var row = button.closest('[data-backup-job-id]');
    var id = row && row.getAttribute('data-backup-job-id');
    if (!id) { return; }
    var action = button.getAttribute('data-backup-job-action');
    var url = '/api/v1/backup-jobs/' + encodeURIComponent(id);
    if (action === 'edit') {
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
    if (!window.confirm(prompts[action])) { return; }
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
  document.addEventListener('submit', function (event) {
    if (event.target.matches('#backup-job-form')) {
      event.preventDefault(); message().textContent = '';
      var payload = readJob();
      var method = jobID ? 'PUT' : 'POST';
      var url = '/api/v1/backup-jobs' + (jobID ? '/' + encodeURIComponent(jobID) : '');
      window.strandApi(method, url, payload).then(function (resp) { return json(resp, 'Could not save backup job'); })
        .then(function () { location.reload(); }).catch(function (err) { showError(err.message); });
      return;
    }
    if (event.target.matches('#restore-form')) {
      event.preventDefault();
      var f = event.target.elements;
      var get = function (n) { return String(f.namedItem(n).value || '').trim(); };
      var out = document.getElementById('restore-message');
      var body = { backup_id: get('backup_id'), source: get('source'), target_time: isoUTC(get('target_time')),
        target: { host: get('target_host'), port: Number(get('target_port')) || 0, user: get('target_user'), password_env: get('target_password_env') },
        target_database: get('target_database'), target_dir: get('target_dir'), confirm: get('confirm') };
      if (body.confirm !== body.target_database + body.target_dir) { out.textContent = 'Type the target database or directory exactly to confirm.'; return; }
      if (!window.confirm('Start the restore into ' + body.confirm + '? The source is not modified.')) { return; }
      window.strandApi('POST', '/api/v1/backup-restores', body).then(function (resp) { return go(resp, 'Could not start restore'); })
        .catch(function (err) { out.textContent = err.message; });
      return;
    }
    if (event.target.matches('#repo-form')) {
      event.preventDefault();
      var rf = event.target.elements;
      var g = function (n) { return String(rf.namedItem(n).value || '').trim(); };
      var repo = { name: g('name'), env_prefix: g('env_prefix'), bucket: g('bucket'), prefix: g('prefix'), lock_mode: g('lock_mode'),
        lock_days: Number(g('lock_days')) || 0, retention: { keep_daily: Number(g('keep_daily')) || 0, max_age_days: Number(g('max_age_days')) || 0 } };
      window.strandApi('POST', '/api/v1/backup-repositories', repo).then(function (resp) { return json(resp, 'Could not save repository'); })
        .then(function () { location.reload(); }).catch(function (err) { document.getElementById('repo-message').textContent = err.message; });
    }
  });
  document.addEventListener('input', function (event) {
    var f = event.target.form;
    if (f && (f.id === 'restore-form' || f.id === 'repo-form' || f.id === 'backup-job-form')) { f.setAttribute('data-dirty', '1'); }
  });
  syncMethod();
}());
