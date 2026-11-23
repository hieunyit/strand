// Backup console P2/P3: drawers, connection and policy forms, schedule and retention widgets, job actions.
// No libraries. Every write goes through window.strandApi (CSRF) and the RBAC-checked JSON API.
(function () {
  'use strict';
  if (!window.strandApi) { return; }
  var api = window.strandApi;
  var $ = function (sel, root) { return (root || document).querySelector(sel); };
  var $$ = function (sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); };

  function toast(msg, ok) {
    if (window.strandToast) { window.strandToast(msg, ok); }
  }
  function readJSON(resp, fallback) {
    return resp.json().catch(function () { return {}; }).then(function (body) {
      if (!resp.ok) { var e = new Error(body.error || fallback || 'Request failed'); e.status = resp.status; e.body = body; throw e; }
      return body;
    });
  }
  function confirmDlg(message, retype) {
    if (window.strandConfirm) { return window.strandConfirm(message, retype); }
    return Promise.resolve(window.confirm(message));
  }
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text !== undefined) { e.textContent = text; }
    return e;
  }
  function busy(btn, on) {
    if (!btn) { return; }
    btn.disabled = !!on;
    btn.classList.toggle('is-busy', !!on);
    if (on) { btn.setAttribute('aria-busy', 'true'); } else { btn.removeAttribute('aria-busy'); }
  }
  function setHidden(node, hide) {
    node.hidden = !!hide;
    $$('input, select, textarea', node).forEach(function (c) { c.disabled = !!hide; });
    if (node.matches && node.matches('input, select, textarea')) { node.disabled = !!hide; }
  }

  // ---- field errors (inline, attached to the right .field) ----
  function clearErrors(form) {
    $$('.field-error', form).forEach(function (s) { s.textContent = ''; s.hidden = true; });
    $$('[aria-invalid]', form).forEach(function (i) { i.removeAttribute('aria-invalid'); });
  }
  function fieldError(form, name, msg) {
    var input = form.elements.namedItem(name);
    if (!input || (input.length && !input.tagName)) { return false; }
    var wrap = input.closest('.field') || input.parentElement;
    var span = wrap && wrap.querySelector('.field-error');
    if (!span) { span = el('span', 'field-error'); wrap.appendChild(span); }
    span.textContent = msg; span.hidden = false;
    input.setAttribute('aria-invalid', 'true');
    return true;
  }
  var ERR_FIELDS = [
    [/known_hosts/i, 'ssh_known_hosts_ref'], [/identity_file|identity_ref/i, 'ssh_identity_ref'],
    [/ssh_tunnel|ssh/i, 'ssh_host'], [/ca_cert/i, 'ca_cert_pem'], [/tls/i, 'tls_mode'], [/password|secret/i, 'password'],
    [/uri/i, 'uri'], [/port/i, 'port'], [/host/i, 'host'], [/engine/i, 'engine'], [/name/i, 'name']
  ];
  // Maps an API error message onto a field of the form when it names one; returns the field or ''.
  function attachError(form, msg, extra) {
    var table = (extra || []).concat(ERR_FIELDS);
    for (var i = 0; i < table.length; i++) {
      if (table[i][0].test(msg) && form.elements.namedItem(table[i][1]) && !form.elements.namedItem(table[i][1]).disabled) {
        if (fieldError(form, table[i][1], msg)) { return table[i][1]; }
      }
    }
    return '';
  }
  function showFormError(form, alertEl, msg, extra) {
    clearErrors(form);
    var f = attachError(form, msg, extra);
    if (alertEl) { alertEl.textContent = f ? '' : msg; alertEl.hidden = !!f; }
    var first = f ? form.elements.namedItem(f) : null;
    if (first && first.focus) { first.focus(); if (first.scrollIntoView) { first.scrollIntoView({ block: 'center' }); } }
    else if (alertEl && alertEl.scrollIntoView) { alertEl.scrollIntoView({ block: 'nearest' }); }
    return f;
  }
  window.strandBackupErr = { attach: attachError, show: showFormError, clear: clearErrors, field: fieldError };

  // ---- drawers: <dialog class="drawer"> ----
  document.addEventListener('click', function (ev) {
    var t = ev.target;
    var open = t.closest && t.closest('[data-open-drawer]');
    if (open) {
      var d = document.getElementById(open.getAttribute('data-open-drawer'));
      if (d && typeof d.showModal === 'function' && !d.open) { d.showModal(); }
      return;
    }
    var close = t.closest && t.closest('[data-close-drawer]');
    if (close) { closeDrawer(close.closest('dialog')); }
  });
  function closeDrawer(d) {
    if (!d) { return; }
    var f = d.querySelector('form');
    if (f && f.hasAttribute('data-dirty') && !window.confirm('Discard the changes you made in this form?')) { return; }
    if (f) { f.removeAttribute('data-dirty'); }
    d.close();
  }
  $$('dialog.drawer').forEach(function (d) {
    d.addEventListener('cancel', function (ev) {
      var f = d.querySelector('form');
      if (f && f.hasAttribute('data-dirty') && !window.confirm('Discard the changes you made in this form?')) { ev.preventDefault(); }
    });
    d.addEventListener('click', function (ev) { if (ev.target === d) { closeDrawer(d); } });
  });

  // ---- schedule widget ----
  var DAY_NAMES = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  function sched(root) { return root.querySelector('[data-schedule]') || root; }
  function sv(root, name) { var c = sched(root).querySelector('[name="' + name + '"]'); return c ? String(c.value || '').trim() : ''; }
  function parseDays(text, kind) {
    var out = [];
    String(text || '').split(/[\s,]+/).filter(Boolean).forEach(function (p) {
      var n = DAY_NAMES.indexOf(p.slice(0, 3).toLowerCase());
      if (kind === 'weekly' && n >= 0 && isNaN(Number(p))) { out.push(n); return; }
      var v = Number(p);
      if (isFinite(v)) { out.push(v); }
    });
    return out;
  }
  function readSchedule(root) {
    var kind = sv(root, 'sch_kind') || 'daily';
    var s = { kind: kind };
    var tz = sv(root, 'sch_tz');
    if (kind === 'interval') { s.every_seconds = Math.round((Number(sv(root, 'sch_every_min')) || 0) * 60); }
    if (kind === 'daily' || kind === 'weekly' || kind === 'monthly') { s.at = sv(root, 'sch_at'); }
    if (kind === 'weekly' || kind === 'monthly') { s.days = parseDays(sv(root, 'sch_days'), kind); }
    if (kind === 'cron') { s.cron = sv(root, 'sch_cron'); }
    if (tz) { s.timezone = tz; }
    return s;
  }
  function writeSchedule(root, s) {
    var base = sched(root);
    function set(n, v) { var c = base.querySelector('[name="' + n + '"]'); if (c) { c.value = v; } }
    s = s || {};
    set('sch_kind', s.kind || 'daily');
    set('sch_at', s.at || '01:00');
    // The API stores whole seconds while the widget displays minutes.
    set('sch_every_min', s.every_seconds ? Number((s.every_seconds / 60).toFixed(4)) : 60);
    set('sch_cron', s.cron || '');
    set('sch_days', (s.days || []).join(','));
    set('sch_tz', s.timezone || '');
    syncSchedule(root);
  }
  function syncSchedule(root) {
    var base = sched(root), kind = sv(root, 'sch_kind') || 'daily';
    $$('[data-sched-kinds]', base).forEach(function (n) {
      setHidden(n, n.getAttribute('data-sched-kinds').split(/\s+/).indexOf(kind) < 0);
    });
    var hint = $('[data-sched-days-hint]', base);
    if (hint) { hint.textContent = kind === 'weekly' ? 'Weekdays: 0 = Sunday ... 6 = Saturday (or Mon, Wed).' : kind === 'monthly' ? 'Days of the month, 1-31.' : ''; }
  }
  function validateSchedule(root) {
    var s = readSchedule(root), out = {};
    if ((s.kind === 'daily' || s.kind === 'weekly' || s.kind === 'monthly') && !/^\d\d:\d\d$/.test(s.at || '')) { out.sch_at = 'Enter the time as HH:MM.'; }
    if (s.kind === 'interval' && !(s.every_seconds >= 60)) { out.sch_every_min = 'The interval must be at least 1 minute.'; }
    if (s.kind === 'cron' && !s.cron) { out.sch_cron = 'Enter a 5-field cron expression.'; }
    if ((s.kind === 'weekly' || s.kind === 'monthly') && !(s.days && s.days.length)) { out.sch_days = 'Choose at least one day.'; }
    return out;
  }
  var previewTimers = new WeakMap();
  function previewSchedule(root) {
    var base = sched(root), list = $('[data-sched-preview]', base);
    if (!list) { return Promise.resolve(); }
    var errs = validateSchedule(root);
    list.textContent = '';
    if (Object.keys(errs).length) { list.appendChild(el('li', 'muted', 'Complete the schedule to preview it.')); return Promise.resolve(); }
    var s = readSchedule(root);
    s.count = 5;
    return api('POST', '/api/v1/backup-schedules/preview', s).then(function (r) { return readJSON(r, 'Could not preview the schedule'); }).then(function (res) {
      list.textContent = '';
      (res.runs || []).forEach(function (r) { list.appendChild(el('li', '', String(r.local).replace('T', ' ').replace(/(\.\d+)?(Z|[+-]\d\d:\d\d)$/, ' $2'))); });
      if (!(res.runs || []).length) { list.appendChild(el('li', 'muted', 'No upcoming runs.')); }
    }).catch(function (e) {
      list.textContent = '';
      list.appendChild(el('li', 'field-error', e.message));
    });
  }
  function schedChanged(root, cb) {
    syncSchedule(root);
    var t = previewTimers.get(root);
    clearTimeout(t);
    previewTimers.set(root, setTimeout(function () { previewSchedule(root); }, 250));
    if (cb) { cb(); }
  }
  function bindSchedule(root, cb) {
    var base = sched(root);
    syncSchedule(root);
    base.addEventListener('input', function () { schedChanged(root, cb); });
    base.addEventListener('change', function () { schedChanged(root, cb); });
  }

  // ---- retention widget ----
  var RET = [['ret_last', 'keep_last'], ['ret_hourly', 'keep_hourly'], ['ret_daily', 'keep_daily'], ['ret_weekly', 'keep_weekly'],
    ['ret_monthly', 'keep_monthly'], ['ret_yearly', 'keep_yearly'], ['ret_within', 'keep_within_days'], ['ret_maxage', 'max_age_days']];
  function ret(root) { return root.querySelector('[data-retention]') || root; }
  function readRetention(root) {
    var out = {};
    RET.forEach(function (p) {
      var c = root.querySelector('[name="' + p[0] + '"]');
      var n = c ? Math.max(0, Math.floor(Number(c.value) || 0)) : 0;
      if (n > 0) { out[p[1]] = n; }
    });
    return out;
  }
  function writeRetention(root, r) {
    r = r || {};
    RET.forEach(function (p) { var c = root.querySelector('[name="' + p[0] + '"]'); if (c) { c.value = r[p[1]] || 0; } });
    updateWords(root);
  }
  // Mirrors retentionWords in backups_ui_p23.go.
  function retentionWords(r) {
    r = r || {};
    var parts = [], add = function (n, l) { if (n > 0) { parts.push(n + ' ' + l); } };
    add(r.keep_last, 'most recent'); add(r.keep_hourly, 'hourly'); add(r.keep_daily, 'daily');
    add(r.keep_weekly, 'weekly'); add(r.keep_monthly, 'monthly'); add(r.keep_yearly, 'yearly');
    var out = '';
    if (!parts.length && !(r.keep_within_days > 0)) { out = 'Keep every backup'; }
    else if (parts.length) { out = 'Keep ' + parts.join(', ') + ' backups'; }
    if (r.keep_within_days > 0) { out += (out ? ' and' : 'Keep') + ' every backup of the last ' + r.keep_within_days + ' days'; }
    if (r.max_age_days > 0) { out += '; expire anything older than ' + r.max_age_days + ' days'; }
    return out;
  }
  function updateWords(root) {
    var w = $('[data-retention-words]', root);
    if (w) { w.textContent = retentionWords(readRetention(root)); }
  }
  function bindRetention(root, cb) {
    updateWords(root);
    root.addEventListener('input', function (ev) {
      if (ev.target && ev.target.name && ev.target.name.indexOf('ret_') === 0) { updateWords(root); if (cb) { cb(); } }
    });
  }
  window.strandBackupUI = { readSchedule: readSchedule, writeSchedule: writeSchedule, bindSchedule: bindSchedule, validateSchedule: validateSchedule,
    previewSchedule: previewSchedule, syncSchedule: syncSchedule, readRetention: readRetention, writeRetention: writeRetention, retentionWords: retentionWords,
    bindRetention: bindRetention, renderTest: renderTest, connBody: connBody, setHidden: setHidden, busy: busy, readJSON: readJSON, el: el };

  // ---- connection test checklist ----
  var MARK = { ok: '\u2713', warn: '!', fail: '\u2717' };
  function renderTest(box, res) {
    box.textContent = '';
    box.hidden = false;
    var ul = el('ul', 'test-checklist');
    function item(status, name, detail) {
      var li = el('li', status);
      li.appendChild(el('span', 'tc-mark', MARK[status] || '?'));
      var s = el('span');
      s.appendChild(el('b', '', name));
      if (detail) { s.appendChild(el('span', 'tc-detail', ' ' + detail)); }
      li.appendChild(s);
      ul.appendChild(li);
    }
    if (!res.ok) {
      if ((res.checks || []).length) {
        (res.checks || []).forEach(function (c) {
          item(c.status === 'warn' || c.status === 'fail' ? c.status : 'ok', c.name.charAt(0).toUpperCase() + c.name.slice(1).replace(/_/g, ' '), c.detail || '');
        });
      } else {
        item('fail', 'Connect', res.error || 'The server could not be reached.');
      }
      box.appendChild(ul);
      return;
    }
    var seen = {};
    (res.checks || []).forEach(function (c) {
      seen[String(c.name).toLowerCase()] = true;
      item(c.status === 'warn' || c.status === 'fail' ? c.status : 'ok', c.name.charAt(0).toUpperCase() + c.name.slice(1).replace(/_/g, ' '), c.detail || '');
    });
    var miss = res.missing_privileges || [];
    if (!seen.connect) { item('ok', 'Connect', (res.version ? 'Server ' + res.version : '') + (res.latency_ms ? ' in ' + res.latency_ms + ' ms' : '')); }
    if (!seen.privileges) { item(miss.length ? 'warn' : 'ok', 'Privileges', miss.length ? 'Missing: ' + miss.join(', ') : 'All required privileges are granted'); }
    else if (miss.length && !(res.checks || []).some(function (c) { return /privileges/i.test(c.name) && c.detail && c.detail.indexOf(miss[0]) >= 0; })) { item('warn', 'Missing privileges', miss.join(', ')); }
    if (res.gtid_mode && !seen.gtid_mode) { item(String(res.gtid_mode).toUpperCase() === 'ON' ? 'ok' : 'warn', 'GTID mode', res.gtid_mode); }
    if (res.wal_level && !seen.wal_level) { item(res.wal_level === 'replica' || res.wal_level === 'logical' ? 'ok' : 'warn', 'wal_level', res.wal_level); }
    if (res.replica_set && !seen.replica_set) { item('ok', 'Replica set', res.replica_set); }
    if (!seen.databases) { item('ok', 'Databases', (res.databases || []).length + ' found'); }
    item(res.pitr_allowed ? 'ok' : 'warn', 'Point-in-time recovery', res.pitr_allowed ? 'Available' + ((res.pitr_methods || []).length ? ' with ' + res.pitr_methods.join(', ') : '') : (res.pitr_reason || 'Not available'));
    box.appendChild(ul);
  }

  // ---- connection form ----
  var DEFAULT_PORT = { mysql: 3306, mariadb: 3306, postgres: 5432, mongodb: 27017 };
  function connForm() { return document.getElementById('conn-form'); }
  function syncConn(f) {
    if (!f) { return; }
    var engine = f.elements.namedItem('engine').value;
    $$('[data-engines]', f).forEach(function (n) { setHidden(n, n.getAttribute('data-engines').split(/\s+/).indexOf(engine) < 0); });
    var port = f.elements.namedItem('port'), cur = Number(port.value) || 0;
    port.placeholder = String(DEFAULT_PORT[engine] || '');
    if (!f.hasAttribute('data-port-touched') && (cur === 0 || [3306, 5432, 27017].indexOf(cur) >= 0)) { port.value = DEFAULT_PORT[engine] || ''; }
    var req = $('[data-host-req]', f);
    if (req) { req.hidden = engine === 'mongodb'; }
  }
  function connBody(f, forTest) {
    var e = f.elements, g = function (n) { var c = e.namedItem(n); return c && !c.disabled ? String(c.value || '').trim() : ''; };
    var engine = g('engine');
    var body = { name: g('name'), engine: engine, host: g('host'), port: Number(g('port')) || 0, user: g('user'), secret_mode: 'stored',
      tls_mode: g('tls_mode'), ca_cert_pem: g('ca_cert_pem') };
    var pw = e.namedItem('password'), uri = e.namedItem('uri');
    if (pw && !pw.disabled && pw.value !== '') { body.password = pw.value; }
    if (uri && !uri.disabled && engine === 'mongodb' && uri.value !== '') { body.uri = uri.value; }
    if (g('ssh_host')) {
      body.ssh_tunnel = { host: g('ssh_host'), port: Number(g('ssh_port')) || 0, user: g('ssh_user'), identity_ref: g('ssh_identity_ref'), known_hosts_ref: g('ssh_known_hosts_ref') };
    }
    var id = f.getAttribute('data-conn-id');
    if (forTest && id) { body.id = id; }
    return body;
  }
  function validateConn(f) {
    var b = connBody(f), errs = {};
    if (!b.name) { errs.name = 'Name is required.'; }
    var hasURI = b.engine === 'mongodb' && (b.uri || f.getAttribute('data-conn-id'));
    if (!b.host && !hasURI) { errs.host = 'Host is required.'; }
    if (b.port && (b.port < 1 || b.port > 65535)) { errs.port = 'Port must be between 1 and 65535.'; }
    if (b.ssh_tunnel && b.ssh_tunnel.host) {
      if (!b.ssh_tunnel.user) { errs.ssh_user = 'SSH user is required.'; }
      if (!b.ssh_tunnel.known_hosts_ref) { errs.ssh_known_hosts_ref = 'known_hosts file is required.'; }
    }
    return errs;
  }
  function applyErrs(f, errs) {
    clearErrors(f);
    var first = '';
    Object.keys(errs).forEach(function (k) { fieldError(f, k, errs[k]); first = first || k; });
    if (first) { var i = f.elements.namedItem(first); if (i && i.focus) { i.focus(); } }
    return !!first;
  }
  function initConnForm() {
    var f = connForm();
    if (!f) { return; }
    var alertEl = $('#conn-form-error'), status = $('#conn-form-status'), result = $('#conn-test-result');
    syncConn(f);
    f.removeAttribute('data-dirty');
    f.addEventListener('change', function (ev) { if (ev.target.name === 'engine') { syncConn(f); } });
    f.addEventListener('input', function (ev) { if (ev.target.name === 'port') { f.setAttribute('data-port-touched', '1'); } });
    f.elements.namedItem('name').addEventListener('blur', function () { var errs = validateConn(f); if (errs.name) { fieldError(f, 'name', errs.name); } });
    f.elements.namedItem('host').addEventListener('blur', function () { var errs = validateConn(f); if (errs.host) { fieldError(f, 'host', errs.host); } });
    $('#conn-test-btn', f).addEventListener('click', function () {
      var btn = this, errs = validateConn(f);
      delete errs.name; // a test does not need a name
      if (applyErrs(f, errs)) { return; }
      alertEl.hidden = true; status.textContent = 'Testing...';
      busy(btn, true);
      api('POST', '/api/v1/backup-connections/test', connBody(f, true)).then(function (r) { return readJSON(r, 'Test failed'); }).then(function (res) {
        status.textContent = res.ok ? 'Connection works.' : 'Connection failed.';
        renderTest(result, res);
        f.dispatchEvent(new CustomEvent('strand:conn-tested', { bubbles: true, detail: { result: res } }));
      }).catch(function (e) { status.textContent = ''; showFormError(f, alertEl, e.message); }).then(function () { busy(btn, false); });
    });
    f.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var errs = validateConn(f);
      if (applyErrs(f, errs)) { return; }
      var id = f.getAttribute('data-conn-id'), btn = $('#conn-save-btn', f);
      busy(btn, true); alertEl.hidden = true;
      api(id ? 'PUT' : 'POST', '/api/v1/backup-connections' + (id ? '/' + encodeURIComponent(id) : ''), connBody(f, false))
        .then(function (r) { return readJSON(r, 'Could not save the connection'); })
        .then(function (res) {
          f.removeAttribute('data-dirty');
          if (f.hasAttribute('data-wizard')) {
            f.dispatchEvent(new CustomEvent('strand:conn-saved', { bubbles: true, detail: { connection: res.connection } }));
            f.reset(); syncConn(f); result.hidden = true; f.removeAttribute('data-dirty');
            $('#conn-drawer').close();
            return;
          }
          location.href = '/backups/connections/' + encodeURIComponent(res.connection.id);
        })
        .catch(function (e) { showFormError(f, alertEl, e.message); }).then(function () { busy(btn, false); });
    });
  }
  initConnForm();

  // connection list / detail actions
  document.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('[data-conn-action]');
    if (!b) { return; }
    var id = b.getAttribute('data-id'), act = b.getAttribute('data-conn-action');
    if (act === 'test') {
      busy(b, true);
      api('POST', '/api/v1/backup-connections/' + encodeURIComponent(id) + '/test').then(function (r) { return readJSON(r, 'Test failed'); }).then(function (res) {
        var box = document.getElementById('conn-page-result');
        if (box) { renderTest(box, res); } else { toast(res.ok ? 'Connection works' + (res.version ? ' (' + res.version + ')' : '') : 'Connection failed: ' + (res.error || ''), !!res.ok); }
        if (!box) { setTimeout(function () { location.reload(); }, 1200); }
      }).catch(function (e) { toast(e.message, false); }).then(function () { busy(b, false); });
    } else if (act === 'delete') {
      var name = b.getAttribute('data-name');
      confirmDlg('Delete connection ' + name + '? Jobs that use it must be changed first.', name).then(function (ok) {
        if (!ok) { return; }
        api('DELETE', '/api/v1/backup-connections/' + encodeURIComponent(id)).then(function (r) {
          if (r.status === 204) { location.href = '/backups/connections'; return; }
          return readJSON(r, 'Could not delete').catch(function (e) {
            var jobs = e.body && e.body.jobs ? ' Used by: ' + e.body.jobs.join(', ') : '';
            toast(e.message + '.' + jobs, false);
          });
        });
      });
    }
  });

  // ---- policy form ----
  function policyForm() { return document.getElementById('policy-form'); }
  function pv(f, n) { var c = f.elements.namedItem(n); return c ? String(c.value || '').trim() : ''; }
  function pn(f, n) { return Number(pv(f, n)) || 0; }
  function pc(f, n) { var c = f.elements.namedItem(n); return !!(c && c.checked); }
  function policyBody(f, stored) {
    var body = {};
    if (stored) { Object.keys(stored).forEach(function (k) { body[k] = stored[k]; }); }
    ['id', 'builtin', 'created_by', 'created_at', 'updated_at'].forEach(function (k) { delete body[k]; });
    body.name = pv(f, 'name');
    body.description = pv(f, 'description');
    body.schedule = readSchedule(f);
    body.window_minutes = pn(f, 'window_minutes');
    body.jitter_seconds = pn(f, 'jitter_seconds');
    body.retention = readRetention(f);
    body.log_archive = pc(f, 'log_archive');
    body.log_flush_seconds = body.log_archive ? (body.log_flush_seconds || 300) : 0;
    body.rpo_minutes = body.log_archive ? pn(f, 'rpo_minutes') : (stored ? body.rpo_minutes || 0 : 0);
    body.repository = pv(f, 'repository');
    body.replicate_to = pv(f, 'replicate_to');
    body.compress = pv(f, 'compress') === 'true';
    body.compress_level = Number(pv(f, 'compress_level')) || 0;
    body.encrypt = pc(f, 'encrypt');
    body.verify_every_seconds = pn(f, 'verify_every');
    body.test_restore = pc(f, 'test_restore');
    body.retry_max = pn(f, 'retry_max');
    body.retry_backoff_seconds = body.retry_max > 0 ? pn(f, 'retry_backoff') : 0;
    return body;
  }
  function fillPolicy(f, p) {
    function set(n, v) { var c = f.elements.namedItem(n); if (c) { c.value = v === undefined || v === null ? '' : v; } }
    set('name', p.name); set('description', p.description || '');
    writeSchedule(f, p.schedule);
    set('window_minutes', p.window_minutes || 0); set('jitter_seconds', p.jitter_seconds || 0);
    writeRetention(f, p.retention);
    f.elements.namedItem('log_archive').checked = !!p.log_archive; set('rpo_minutes', p.rpo_minutes || 0);
    set('repository', p.repository || ''); set('replicate_to', p.replicate_to || '');
    set('compress', p.compress ? 'true' : 'false'); set('compress_level', String(p.compress_level || 0));
    f.elements.namedItem('encrypt').checked = !!p.encrypt;
    var v = String(p.verify_every_seconds || 0);
    var sel = f.elements.namedItem('verify_every');
    if (!$$('option', sel).some(function (o) { return o.value === v; }) && p.verify_every_seconds) { var o = el('option', '', 'Every ' + Math.round(p.verify_every_seconds / 60) + ' min'); o.value = v; sel.appendChild(o); }
    sel.value = v;
    f.elements.namedItem('test_restore').checked = !!p.test_restore;
    set('retry_max', p.retry_max || 0); set('retry_backoff', p.retry_backoff_seconds || 60);
  }
  function syncPolicyConditions(f) {
    $$('[data-show-if]', f).forEach(function (n) {
      var c = f.elements.namedItem(n.getAttribute('data-show-if'));
      var on = c && (c.type === 'checkbox' ? c.checked : Number(c.value) > 0);
      setHidden(n, !on);
    });
    var level = $('[data-compress-level]', f);
    if (level) { setHidden(level, pv(f, 'compress') !== 'true'); }
  }
  function initPolicyForm() {
    var f = policyForm();
    if (!f) { return; }
    var alertEl = $('#policy-form-error'), stored = null;
    var raw = f.getAttribute('data-policy-json');
    if (raw) { try { stored = JSON.parse(raw); } catch (e) { stored = null; } }
    bindSchedule(f, function () { f.setAttribute('data-dirty', '1'); });
    bindRetention(f);
    if (stored) { fillPolicy(f, stored); }
    else { writeRetention(f, { keep_daily: 7, keep_weekly: 4 }); }
    syncPolicyConditions(f);
    previewSchedule(f);
    f.removeAttribute('data-dirty');
    f.addEventListener('input', function () { syncPolicyConditions(f); });
    f.addEventListener('change', function () { syncPolicyConditions(f); });
    function validate() {
      var errs = validateSchedule(f);
      if (!pv(f, 'name')) { errs.name = 'Name is required.'; }
      return errs;
    }
    f.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var errs = validate();
      if (applyErrs(f, errs)) { return; }
      var id = f.getAttribute('data-policy-id'), btn = $('#policy-save-btn', f), body = policyBody(f, stored);
      busy(btn, true); alertEl.hidden = true;
      function done(res) {
        f.removeAttribute('data-dirty');
        if (id) { location.reload(); } else { location.href = '/backups/policies/' + encodeURIComponent(res.policy.id); }
      }
      function fail(e) { showFormError(f, alertEl, e.message, [[/schedule|cron|interval|time/i, 'sch_kind'], [/retention|keep/i, 'ret_daily']]); }
      if (!id) {
        api('POST', '/api/v1/backup-policies', body).then(function (r) { return readJSON(r, 'Could not create the policy'); }).then(done).catch(fail).then(function () { busy(btn, false); });
        return;
      }
      api('POST', '/api/v1/backup-policies/' + encodeURIComponent(id) + '/preview', body).then(function (r) { return readJSON(r, 'Could not check the impact'); }).then(function (pre) {
        var n = pre.affects_jobs || 0;
        var go = n === 0 ? Promise.resolve(true) : confirmDlg('Affects ' + n + ' job' + (n === 1 ? '' : 's') + ': ' + (pre.jobs || []).map(function (j) {
          return j.name + (j.changes && j.changes.length ? ' (' + j.changes.join(', ') + ')' : ' (no change)');
        }).join('; ') + '. Save this policy?');
        return go.then(function (ok) {
          if (!ok) { return null; }
          return api('PUT', '/api/v1/backup-policies/' + encodeURIComponent(id), body).then(function (r) { return readJSON(r, 'Could not save the policy'); }).then(done);
        });
      }).catch(fail).then(function () { busy(btn, false); });
    });
  }
  initPolicyForm();
  document.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('[data-policy-action="delete"]');
    if (!b) { return; }
    var name = b.getAttribute('data-name'), id = b.getAttribute('data-id');
    confirmDlg('Delete policy ' + name + '? Jobs that follow it must be changed first.', name).then(function (ok) {
      if (!ok) { return; }
      api('DELETE', '/api/v1/backup-policies/' + encodeURIComponent(id)).then(function (r) {
        if (r.status === 204) { location.href = '/backups/policies'; return; }
        return readJSON(r, 'Could not delete').catch(function (e) {
          toast(e.message + (e.body && e.body.jobs ? '. Used by: ' + e.body.jobs.join(', ') : ''), false);
        });
      });
    });
  });

  // ---- backup job list / detail actions ----
  function jobCall(act, id) {
    var base = '/api/v1/backup-jobs/' + encodeURIComponent(id);
    if (act === 'delete') { return api('DELETE', base); }
    return api('POST', base + '/' + act);
  }
  function okStatus(r) { return r.ok ? r : readJSON(r, 'Request failed'); }
  document.addEventListener('click', function (ev) {
    var b = ev.target.closest && ev.target.closest('[data-job-action]');
    if (b) {
      var act = b.getAttribute('data-job-action'), id = b.getAttribute('data-id');
      var go = act === 'delete' ? confirmDlg('Delete this backup job definition? Existing backups stay stored.', b.getAttribute('data-name') || id) : Promise.resolve(true);
      go.then(function (ok) {
        if (!ok) { return; }
        busy(b, true);
        jobCall(act, id).then(okStatus).then(function () {
          toast(act === 'run' ? 'Backup started' : act === 'delete' ? 'Job deleted' : 'Job ' + act + 'd', true);
          setTimeout(function () { if (act === 'delete' && /\/backups\/jobs\/[^/]+$/.test(location.pathname)) { location.href = '/backups/jobs'; } else { location.reload(); } }, 900);
        }).catch(function (e) { toast(e.message, false); busy(b, false); });
      });
      return;
    }
    var rc = ev.target.closest && ev.target.closest('[data-run-action="cancel"]');
    if (rc) {
      busy(rc, true);
      api('POST', '/api/v1/backup-runs/' + encodeURIComponent(rc.getAttribute('data-id')) + '/cancel').then(okStatus).then(function () {
        toast('Run cancelled', true); setTimeout(function () { location.reload(); }, 900);
      }).catch(function (e) { toast(e.message, false); busy(rc, false); });
      return;
    }
    var cv = ev.target.closest && ev.target.closest('[data-convert-job]');
    if (cv) {
      confirmDlg('Convert the inline connection of this job into a saved connection? The job keeps working unchanged.').then(function (ok) {
        if (!ok) { return; }
        busy(cv, true);
        api('POST', '/api/v1/backup-jobs/' + encodeURIComponent(cv.getAttribute('data-convert-job')) + '/convert-connection').then(function (r) { return readJSON(r, 'Could not convert'); }).then(function (res) {
          toast('Converted to connection ' + res.connection.name, true); setTimeout(function () { location.reload(); }, 900);
        }).catch(function (e) { toast(e.message, false); busy(cv, false); });
      });
    }
  });

  // bulk selection on the jobs list
  var bulk = document.getElementById('backup-jobs-bulk');
  if (bulk) {
    var boxes = function () { return $$('input[name="job_select"]'); };
    var sync = function () {
      var sel = boxes().filter(function (c) { return c.checked; });
      bulk.hidden = sel.length === 0;
      $('#bulk-selected-count').textContent = sel.length + ' selected';
    };
    document.addEventListener('change', function (ev) {
      if (ev.target.id === 'select-all-jobs') { boxes().forEach(function (c) { c.checked = ev.target.checked; }); }
      if (ev.target.name === 'job_select' || ev.target.id === 'select-all-jobs') { sync(); }
    });
    bulk.addEventListener('click', function (ev) {
      var b = ev.target.closest('[data-bulk-action]');
      if (!b) { return; }
      var act = b.getAttribute('data-bulk-action');
      var ids = boxes().filter(function (c) { return c.checked; }).map(function (c) { return c.value; });
      if (!ids.length) { return; }
      var phrase = 'delete ' + ids.length + ' jobs';
      var go = act === 'delete' ? confirmDlg('Delete ' + ids.length + ' backup job definition(s)? Existing backups stay stored.', phrase) : Promise.resolve(true);
      go.then(function (ok) {
        if (!ok) { return; }
        busy(b, true);
        var failed = [];
        ids.reduce(function (p, id) {
          return p.then(function () { return jobCall(act, id).then(okStatus).catch(function (e) { failed.push(id + ': ' + e.message); }); });
        }, Promise.resolve()).then(function () {
          if (failed.length) { toast(failed.length + ' failed. ' + failed[0], false); busy(b, false); setTimeout(function () { location.reload(); }, 2500); return; }
          toast(ids.length + ' job(s): ' + act + ' done', true); setTimeout(function () { location.reload(); }, 900);
        });
      });
    });
    sync();
  }
}());
