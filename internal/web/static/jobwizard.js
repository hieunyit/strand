// Backup job wizard (spec 4.3): 4 steps, live summary, test-gated source step, PITR gating from the
// connection test, override badges against the chosen policy. No libraries; all writes via strandApi.
(function () {
  'use strict';
  var root = document.getElementById('job-wizard');
  var U = window.strandBackupUI;
  if (!root || !window.strandApi || !U) { return; }
  var api = window.strandApi;
  var $ = function (s, r) { return (r || root).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || root).querySelectorAll(s)); };
  var form = document.getElementById('job-wizard-form');
  var D;
  try { D = JSON.parse(root.getAttribute('data-wizard') || '{}'); } catch (e) { D = {}; }
  D.connections = D.connections || []; D.policies = D.policies || []; D.repos = D.repos || [];
  D.tools = D.tools || {}; D.method_tools = D.method_tools || {}; D.test_restore_tools = D.test_restore_tools || {};
  var EDIT = !!D.edit, JOB = D.job || null;
  var el = U.el;
  var STEPS = 4;

  // Methods offered per connection engine. Custom scripts live under Advanced.
  var METHODS = {
    mysql: [{ v: 'mysqldump', d: 'Logical dump streamed to the repository. Best up to about 50 GB.' }, { v: 'mydumper', d: 'Parallel logical dump. Faster above 50 GB.' }],
    mariadb: [{ v: 'mariadb-dump', d: 'Logical dump with the MariaDB client, streamed to the repository.' }, { v: 'mydumper', d: 'Parallel logical dump. Faster for large databases.' }],
    postgres: [{ v: 'pg_dump', d: 'Logical dump of the chosen databases (at least one).' }, { v: 'pg_basebackup', d: 'Physical copy of the whole cluster. The base for point-in-time recovery.' }],
    mongodb: [{ v: 'mongodump', d: 'BSON dump of one database or the whole replica set.' }]
  };
  var TEST_RESTORE_METHODS = ['mysqldump', 'mariadb-dump', 'pg_dump'];
  var OVERRIDABLE = ['schedule', 'window_minutes', 'jitter_seconds', 'retention', 'log_archive', 'log_flush_seconds', 'repository', 'replicate_to', 'compress',
    'compress_level', 'encrypt', 'verify_every_seconds', 'verify_sample_percent', 'test_restore', 'retry_max', 'retry_backoff_seconds'];

  var S = {
    step: 1, reached: 1, done: {}, test: null, testConn: '', methodTouched: false, nameTouched: false,
    touched: {}, policy: '', custom: false, submitting: false, dbs: [], dbsAll: true, savedJobID: ''
  };

  // ---- small accessors ----
  function f(name) { return form.elements.namedItem(name); }
  function v(name) { var c = f(name); return c && !c.disabled ? String(c.value || '').trim() : ''; }
  function n(name) { return Number(v(name)) || 0; }
  function chk(name) { var c = f(name); return !!(c && c.checked && !c.disabled); }
  function setv(name, val) { var c = f(name); if (c) { c.value = val === undefined || val === null ? '' : val; } }
  function setc(name, on) { var c = f(name); if (c) { c.checked = !!on; } }
  function connById(id) { return D.connections.filter(function (c) { return c.id === id; })[0]; }
  function policyById(id) { return D.policies.filter(function (p) { return p.id === id; })[0]; }
  function repoById(id) { return D.repos.filter(function (r) { return r.id === id; })[0]; }
  function isCustomJob() { return chk('use_custom'); }
  function conn() { return connById(v('connection_id')); }
  function engine() { var c = conn(); return c ? c.engine : ''; }
  function method() { return isCustomJob() ? 'custom' : (checkedRadio('method') || ''); }
  function checkedRadio(name) { var c = form.querySelector('input[name="' + name + '"]:checked'); return c ? c.value : ''; }
  function selectedDBs() { return S.dbsAll ? [] : S.dbs.slice(); }
  function policy() { return S.custom ? null : policyById(S.policy); }
  function customSchedule() { return S.custom || chk('customize'); }

  // ---- PITR gating (pure; reasons mirror backup.ValidatePITR) ----
  function pitrSupport(test, meth, dbs) {
    if (meth === 'custom') { return { ok: false, reason: 'Custom scripts do not support point-in-time recovery.' }; }
    if (!test) { return { ok: false, reason: 'Test the connection first: point-in-time recovery depends on the server settings.' }; }
    if (!test.pitr_allowed) { return { ok: false, reason: test.pitr_reason || 'Point-in-time recovery is not available on this server.' }; }
    if (meth === 'mariadb-dump') { return { ok: false, reason: 'MariaDB does not support binlog archiving.' }; }
    if (meth === 'pg_dump') { return { ok: false, reason: 'PITR needs pg_basebackup as the base backup.' }; }
    if (meth === 'mydumper') { return { ok: false, reason: 'mydumper does not support binlog archiving.' }; }
    var ms = test.pitr_methods || [];
    if (meth && ms.length && ms.indexOf(meth) < 0) { return { ok: false, reason: 'PITR is not available with ' + meth + '; use ' + ms.join(' or ') + '.' }; }
    if (meth === 'mysqldump' && (dbs || []).length !== 1) { return { ok: false, reason: 'PITR needs exactly one database.' }; }
    return { ok: true, reason: '' };
  }

  // ---- step 1: connection, test, databases, methods ----
  function buildConnSelect() {
    var sel = f('connection_id'), keep = sel.value;
    while (sel.options.length > 1) { sel.remove(1); }
    D.connections.forEach(function (c) {
      var o = el('option', '', '[' + c.engine + '] ' + c.name + ' (' + c.host + (c.port ? ':' + c.port : '') + ')' + (c.status === 'ok' ? '' : c.status === 'untested' ? ' - never tested' : ' - last test ' + c.status));
      o.value = c.id; sel.appendChild(o);
    });
    sel.value = keep;
    var sc = f('scratch_connection_id');
    while (sc.options.length > 1) { sc.remove(1); }
    D.connections.forEach(function (c) {
      var o = el('option', '', '[' + c.engine + '] ' + c.name + ' (' + c.host + (c.port ? ':' + c.port : '') + ')');
      o.value = c.id; sc.appendChild(o);
    });
  }
  function renderDbs() {
    var box = $('#wiz-dbs'), field = $('#wiz-dbs-field');
    var res = S.test, meth = method();
    var show = !isCustomJob() && !!(res || (EDIT && JOB)) && meth !== 'pg_basebackup';
    field.hidden = !show;
    if (!show) { return; }
    var names = res && res.databases ? res.databases.slice() : [];
    S.dbs.forEach(function (d) { if (names.indexOf(d) < 0) { names.push(d); } });
    box.textContent = '';
    names.forEach(function (d) {
      var lab = el('label', 'check'), cb = el('input');
      cb.type = 'checkbox'; cb.value = d; cb.name = 'db'; cb.checked = S.dbs.indexOf(d) >= 0 && !S.dbsAll; cb.disabled = S.dbsAll;
      lab.appendChild(cb); lab.appendChild(el('span', '', d)); box.appendChild(lab);
    });
    $('#wiz-db-all').checked = S.dbsAll;
    var hint = $('#wiz-dbs-hint');
    hint.textContent = meth === 'pg_dump' ? 'pg_dump needs at least one database.' : meth === 'mongodump' ? 'One database, or all of them.'
      : meth === 'mysqldump' ? 'Point-in-time recovery needs exactly one database.' : 'Leave "All databases" ticked to back up everything.';
  }
  function renderMethods() {
    var box = $('#wiz-methods'), field = $('#wiz-methods-field'), eng = engine();
    field.hidden = !eng || isCustomJob();
    box.textContent = '';
    if (!eng) { return; }
    var cur = checkedRadio('method') || (S.methodPref || '');
    var list = METHODS[eng] || [];
    var rec = recommended(eng);
    if (!cur || !list.some(function (m) { return m.v === cur; })) { cur = rec; }
    list.forEach(function (m) {
      var lab = el('label', 'choice'), r = el('input');
      r.type = 'radio'; r.name = 'method'; r.value = m.v; r.checked = m.v === cur;
      lab.appendChild(r);
      var t = el('span', 'choice-title', m.v);
      if (m.v === rec) { t.appendChild(document.createTextNode(' ')); t.appendChild(el('span', 'badge recommended small', 'Recommended')); }
      lab.appendChild(t); lab.appendChild(el('span', 'choice-desc', m.d));
      box.appendChild(lab);
    });
  }
  function wantsPitr() { return chk('log_archive') || (!!policy() && !!policy().log_archive && !S.touched.log_archive); }
  function recommended(eng) {
    if (eng === 'postgres') { return wantsPitr() ? 'pg_basebackup' : 'pg_dump'; }
    return (METHODS[eng] || [{}])[0].v;
  }
  function onConnChange() {
    var c = conn(), btn = $('#wiz-test-btn');
    btn.disabled = !c;
    if (!c || S.testConn !== c.id) { S.test = null; S.testConn = ''; $('#wiz-test-result').hidden = true; }
    $('#wiz-test-status').textContent = c ? (S.test ? 'Connection works.' : 'Test the connection to load its databases.') : 'Choose a connection, then test it.';
    if (!S.methodTouched) { S.methodPref = ''; setRadio('method', ''); }
    S.dbs = S.dbs.filter(function () { return !!S.test; });
    renderMethods(); renderDbs(); syncAll();
    if (!S.nameTouched && c) { setv('name', suggestName()); }
  }
  function setRadio(name, val) { $$('input[name="' + name + '"]').forEach(function (r) { r.checked = r.value === val; }); }
  function suggestName() {
    var c = conn(); if (!c) { return ''; }
    var d = S.dbsAll || !S.dbs.length ? 'all' : S.dbs.length === 1 ? S.dbs[0] : S.dbs.length + 'dbs';
    return (c.name + '-' + d + '-' + (method() || 'backup')).toLowerCase().replace(/[^a-z0-9._-]+/g, '-').slice(0, 96);
  }
  function runTest(id, btn) {
    U.busy(btn, true);
    var st = $('#wiz-test-status'); st.textContent = 'Testing...';
    return api('POST', '/api/v1/backup-connections/' + encodeURIComponent(id) + '/test').then(function (r) { return U.readJSON(r, 'Test failed'); }).then(function (res) {
      if (v('connection_id') !== id) { return; }
      U.renderTest($('#wiz-test-result'), res);
      if (res.ok) {
        S.test = res; S.testConn = id;
        st.textContent = 'Connection works' + (res.version ? ' (' + res.version + ')' : '') + '.';
      } else { S.test = null; S.testConn = ''; st.textContent = 'Connection failed.'; }
      renderMethods(); renderDbs(); syncAll();
      if (!S.nameTouched) { setv('name', suggestName()); updateSummary(); }
    }).catch(function (e) { S.test = null; st.textContent = e.message; }).then(function () { U.busy(btn, false); syncAll(); });
  }

  // ---- step 2: policy cards, schedule, PITR ----
  function renderPolicies() {
    var box = $('#wiz-policies'); box.textContent = '';
    D.policies.forEach(function (p) { box.appendChild(policyCard(p.id, p.name, p.schedule_human, p.retention_words, p)); });
    box.appendChild(policyCard('__custom', 'Custom', 'Own schedule and retention', 'Set every field yourself', null));
    setRadio('policy_choice', S.custom ? '__custom' : S.policy);
  }
  function policyCard(id, title, line1, line2, p) {
    var lab = el('label', 'choice'), r = el('input');
    r.type = 'radio'; r.name = 'policy_choice'; r.value = id;
    lab.appendChild(r); lab.appendChild(el('span', 'choice-title', title));
    lab.appendChild(el('span', 'choice-desc', line1)); lab.appendChild(el('span', 'choice-desc', line2));
    if (p) {
      var flags = [];
      if (p.log_archive) { flags.push('PITR'); }
      if (p.verify_every_seconds) { flags.push('verify'); }
      if (p.test_restore) { flags.push('test restore'); }
      if (flags.length) { lab.appendChild(el('span', 'choice-desc', flags.join(' + '))); }
    }
    return lab;
  }
  function onPolicyChoice(val) {
    S.custom = val === '__custom'; S.policy = S.custom ? '' : val;
    S.touched = {};
    if (!S.custom) { applyPolicyDefaults(); } else { applyCustomDefaults(); }
    renderMethods(); syncAll();
  }
  function applyPolicyDefaults() {
    var p = policy(); if (!p) { return; }
    setc('log_archive', p.log_archive);
    setc('compress', p.compress);
    setv('encryption_mode', 'inherit');
    setv('verify_every', p.verify_every_seconds || 0); setv('verify_sample', p.verify_sample_percent || 100); setc('test_restore', p.test_restore);
    setv('retry_max', p.retry_max || 0); setv('retry_backoff', p.retry_backoff_seconds || 60);
    if (p.repository && !S.touched.repository) { setRepo(p.repository); }
    if (p.replicate_to && !S.touched.replicate_to) { setv('replicate_to', p.replicate_to); }
    setc('customize', false);
    U.writeSchedule(root, p.schedule); setv('window_minutes', p.window_minutes || 0); setv('jitter_seconds', p.jitter_seconds || 0); U.writeRetention(root, p.retention);
    setv('compress_level', p.compress_level || 0);
  }
  function applyCustomDefaults() {
    setc('log_archive', false); setc('compress', true); setv('encryption_mode', '');
    setv('verify_every', 604800); setc('test_restore', false); setv('retry_max', 0); setc('customize', false);
    var tz = v('sch_tz');
    U.writeSchedule(root, { kind: 'daily', at: '01:00', timezone: tz }); setv('window_minutes', 240); setv('jitter_seconds', 0);
    U.writeRetention(root, { keep_daily: 7, keep_weekly: 4 });
  }
  function syncPolicyBlock() {
    var p = policy(), custom = S.custom;
    $('#wiz-customize-row').hidden = custom;
    var showSched = customSchedule();
    $('#wiz-sched-block').hidden = !showSched;
    var words = $('#wiz-policy-words');
    words.hidden = custom || showSched;
    if (p) { words.textContent = p.schedule_human + '. ' + p.retention_words + '.'; }
    var note = $('#wiz-policy-note');
    var msg = '';
    if (p && p.log_archive) {
      var sup = pitrSupport(S.test, method(), selectedDBs());
      msg = sup.ok ? '' : 'This policy asks for point-in-time recovery; it will be off for this job: ' + sup.reason;
    }
    note.textContent = msg;
  }
  function syncPitr() {
    var box = f('log_archive'), reason = $('#wiz-pitr-reason'), sup = pitrSupport(S.test, method(), selectedDBs());
    box.disabled = !sup.ok;
    if (!sup.ok) { box.checked = false; }
    reason.textContent = sup.ok ? 'Archives binlog / WAL / oplog continuously so you can restore to any moment.' : sup.reason;
    $('#wiz-pitr-row').classList.toggle('is-disabled', !sup.ok);
  }

  // ---- step 3 ----
  function setRepo(id) { setRadio('repository', id); }
  function renderRepos() {
    var box = $('#wiz-repos'); box.textContent = '';
    $('#wiz-no-repo').hidden = D.repos.length > 0;
    D.repos.forEach(function (r) {
      var lab = el('label', 'choice'), rd = el('input');
      rd.type = 'radio'; rd.name = 'repository'; rd.value = r.id; lab.appendChild(rd);
      var t = el('span', 'choice-title', r.name || r.id);
      if (r.lock_mode === 'COMPLIANCE') { t.appendChild(document.createTextNode(' ')); t.appendChild(el('span', 'badge tone-immutable small', 'immutable')); }
      lab.appendChild(t);
      lab.appendChild(el('span', 'choice-desc', r.bucket + (r.configured ? '' : ' - credentials not configured')));
      box.appendChild(lab);
    });
    var sel = f('replicate_to');
    while (sel.options.length > 1) { sel.remove(1); }
    D.repos.forEach(function (r) { var o = el('option', '', r.name || r.id); o.value = r.id; sel.appendChild(o); });
    if (D.repos.length === 1 && !checkedRadio('repository')) { setRepo(D.repos[0].id); }
  }
  function effectiveEncryption() {
    return v('encryption_mode') === 'on' || (v('encryption_mode') === 'inherit' && !!(policy() && policy().encrypt));
  }
  function syncEncryption() {
    var select = f('encryption_mode'), p = policy(), enabled = effectiveEncryption();
    var inherit = select.querySelector('option[value="inherit"]');
    inherit.hidden = !p;
    inherit.disabled = !p;
    var blocked = !isCustomJob() && enabled && !D.keys_present;
    $('#wiz-enc-warning').hidden = !blocked;
    if (!EDIT) { $('#wiz-create-only').textContent = blocked ? 'Save disabled draft' : 'Create only'; }
    $('#wiz-enc-reason').textContent = enabled && D.keys_present ? 'New backups use Strand key ' + (D.key_id || 'default') + '.' :
      v('encryption_mode') === 'off' || p && !p.encrypt && v('encryption_mode') === 'inherit' ?
        'Strand encryption is off. The repository may apply its own encryption.' : '';
  }

  // ---- native tools (the server refuses an enabled job whose tool is missing) ----
  var TOOL_HINT = D.tool_hint || 'install it in the Strand Web host/image or set STRAND_BACKUP_TOOLS_DIR';
  function missingTools(list) { return (list || []).filter(function (t) { return !(D.tools[t] && D.tools[t].found); }); }
  function backupToolsMissing() { return isCustomJob() ? [] : missingTools(D.method_tools[method()]); }
  function testRestoreToolsMissing() { return chk('test_restore') && !isCustomJob() ? missingTools(D.test_restore_tools[method()]) : []; }
  function toolMissingText(miss) { return 'Native tool ' + miss.join(', ') + ' not found on the Strand Web host: an enabled job cannot be saved until it is installed.'; }
  function renderToolStatus(box, list) {
    box.textContent = '';
    if (!list || !list.length) { box.hidden = true; return; }
    box.hidden = false;
    list.forEach(function (t) {
      var info = D.tools[t] || {}, row = el('div', 'small');
      row.setAttribute('data-tool', t);
      row.appendChild(el('span', 'badge small ' + (info.found ? 'st-synced' : 'st-failed'), info.found ? 'found' : 'tool missing'));
      row.appendChild(document.createTextNode(' ' + t + (info.found ? (info.version ? ': ' + info.version : '') : '')));
      box.appendChild(row);
    });
    var miss = missingTools(list);
    if (miss.length) { box.appendChild(el('div', 'field-hint', 'Not found: ' + TOOL_HINT.charAt(0).toUpperCase() + TOOL_HINT.slice(1) + '.')); }
  }
  function syncTools() {
    renderToolStatus($('#wiz-tool-status'), isCustomJob() ? [] : D.method_tools[method()]);
    renderToolStatus($('#wiz-tr-tool'), chk('test_restore') && !isCustomJob() ? D.test_restore_tools[method()] : []);
  }

  // ---- step 4 ----
  function syncVerify() {
    var meth = method(), tr = f('test_restore'), ok = TEST_RESTORE_METHODS.indexOf(meth) >= 0;
    tr.disabled = !ok; if (!ok) { tr.checked = false; }
    $('#wiz-tr-row').classList.toggle('is-disabled', !ok);
    $('#wiz-tr-row').hidden = isCustomJob();
    $('#wiz-tr-reason').textContent = ok ? 'After each verification the newest dump is restored into a scratch database.' : 'Only mysqldump, mariadb-dump and pg_dump backups can be test-restored.';
    var sf = $('#wiz-scratch-field'); sf.hidden = !tr.checked;
    f('scratch_connection_id').disabled = !tr.checked;
    var showVerify = n('verify_every') > 0;
    var vs = $('[data-show-verify]'); vs.hidden = !showVerify; f('verify_sample').disabled = !showVerify;
    var rb = $('[data-show-retry]'); rb.hidden = !(n('retry_max') > 0); f('retry_backoff').disabled = !(n('retry_max') > 0);
  }

  function syncCustom() {
    var on = isCustomJob();
    $$('[data-show-custom]').forEach(function (b) { U.setHidden(b, !on); });
    $$('[data-custom-off]').forEach(function (b) { if (on) { b.hidden = true; } else if (b.id !== 'wiz-dbs-field' && b.id !== 'wiz-methods-field') { b.hidden = false; } });
  }

  // ---- overrides ----
  function fieldValue(field) {
    switch (field) {
      case 'log_archive': return chk('log_archive');
      case 'repository': return checkedRadio('repository');
      case 'replicate_to': return v('replicate_to');
      case 'compress': return chk('compress');
      case 'encrypt': return effectiveEncryption();
      case 'verify_every_seconds': return n('verify_every');
      case 'test_restore': return chk('test_restore');
      case 'retry_max': return n('retry_max');
    }
    return null;
  }
  function policyValue(p, field) {
    var map = { log_archive: !!p.log_archive, repository: p.repository || '', replicate_to: p.replicate_to || '', compress: !!p.compress, encrypt: !!p.encrypt,
      verify_every_seconds: p.verify_every_seconds || 0, test_restore: !!p.test_restore, retry_max: p.retry_max || 0 };
    return map[field];
  }
  function overrides() {
    var p = policy(), out = [];
    if (!p) { return out; }
    Object.keys(S.touched).forEach(function (k) {
      if (!S.touched[k] || k === 'encrypt') { return; }
      if ((k === 'repository' && !p.repository) || (k === 'replicate_to' && !p.replicate_to)) { return; }
      if (fieldValue(k) !== policyValue(p, k)) { out.push(k); }
    });
    if (v('encryption_mode') !== 'inherit') { out.push('encrypt'); }
    if (chk('customize')) { out.push('schedule', 'retention'); }
    return out;
  }
  function syncBadges() {
    var o = overrides();
    $$('[data-override]').forEach(function (b) { b.hidden = o.indexOf(b.getAttribute('data-override')) < 0; });
  }

  // ---- model ----
  function lockedRetention() {
    var r = customSchedule() ? U.readRetention(root) : (policy() ? Object.assign({}, policy().retention || {}) : {});
    if (n('lock_days') > 0) { r.lock_days = n('lock_days'); }
    return r;
  }
  function scratchID() { var c = connById(v('scratch_connection_id')); return c ? c.id : ''; }
  var SERVER_FIELDS = ['id', 'next_run_at', 'updated_at', 'run_started_at', 'run_finished_at', 'last_run_status', 'consecutive_failures', 'overrides'];
  function buildPayload() {
    var p = policy(), send = {}, body = {};
    if (EDIT && JOB) {
      Object.keys(JOB).forEach(function (k) { body[k] = JOB[k]; });
      SERVER_FIELDS.concat(OVERRIDABLE).forEach(function (k) { delete body[k]; });
      if (JOB.connection_id && JOB.connection_id !== v('connection_id')) { delete body.source; }
    }
    function want(field) { return !p || S.touched[field]; }
    body.name = v('name'); body.enabled = EDIT && JOB ? JOB.enabled : true;
    if (isCustomJob()) {
      body.method = 'custom'; body.command = v('command'); body.artifact_template = v('artifact_template'); body.manifest_template = v('manifest_template');
      body.bucket = v('bucket'); body.prefix = v('prefix');
      delete body.connection_id;
    } else {
      body.method = method(); body.connection_id = v('connection_id');
      body.conn = { databases: selectedDBs() };
      delete body.command; delete body.artifact_template; delete body.manifest_template;
      var repo = checkedRadio('repository');
      if (!p || !p.repository || S.touched.repository) { body.repository = repo; }
      if (!p || !p.replicate_to || S.touched.replicate_to) { body.replicate_to = v('replicate_to'); }
      if (!p || S.touched.test_restore) { body.test_restore = chk('test_restore'); }
      // The server fills scratch (host, port, user, password reference) from the saved connection.
      if (chk('test_restore')) { body.scratch_connection_id = scratchID(); delete body.scratch; } else { delete body.scratch; delete body.scratch_connection_id; }
    }
    if (p) { body.policy_id = p.id; } else { delete body.policy_id; }
    if (customSchedule()) {
      body.schedule = U.readSchedule(root); body.window_minutes = n('window_minutes'); body.jitter_seconds = n('jitter_seconds');
    }
    if (customSchedule() || n('lock_days') > 0 || !p) { body.retention = lockedRetention(); }
    if (!isCustomJob()) {
      if (want('log_archive')) { body.log_archive = chk('log_archive'); if (body.log_archive) { body.log_flush_seconds = (p && p.log_flush_seconds) || 300; } }
    }
    if (want('compress')) { body.compress = chk('compress'); }
    if (!isCustomJob()) { body.encryption_mode = v('encryption_mode'); }
    if (!p) { body.compress_level = n('compress_level'); } else if (n('compress_level') !== (p.compress_level || 0)) { body.compress_level = n('compress_level'); }
    if (want('verify_every_seconds')) { body.verify_every_seconds = n('verify_every'); if (body.verify_every_seconds) { body.verify_sample_percent = n('verify_sample'); } }
    if (want('retry_max')) { body.retry_max = n('retry_max'); body.retry_backoff_seconds = body.retry_max > 0 ? n('retry_backoff') : 0; }
    body.bandwidth_kbps = n('bandwidth_kbps');
    if (!p && !body.schedule) { body.schedule = U.readSchedule(root); }
    return body;
  }
  window.strandJobWizard = { pitrSupport: pitrSupport, buildPayload: buildPayload, validateStep: function (s) { return validateStep(s); }, state: S, goto: function (s) { return go(s); } };

  // ---- validation per step ----
  function validateStep(step) {
    var e = {}, meth = method();
    if (step === 3 && !isCustomJob() && !v('encryption_mode')) { e.encryption_mode = 'Choose how to protect new backups.'; }
    if (step === 1) {
      if (!v('name')) { e.name = 'Give the job a name.'; }
      if (isCustomJob()) {
        ['command', 'artifact_template', 'manifest_template', 'bucket'].forEach(function (k) { if (!v(k)) { e[k] = 'Required for a custom script.'; } });
      } else {
        if (!v('connection_id')) { e.connection_id = 'Choose a connection.'; }
        else if (!EDIT && !(S.test && S.testConn === v('connection_id'))) { e.connection_id = 'Test the connection successfully before continuing.'; }
        if (v('connection_id') && !meth) { e.method = 'Choose a backup method.'; }
        else if (meth && backupToolsMissing().length) { e.method = toolMissingText(backupToolsMissing()); }
        var dbs = selectedDBs();
        if (meth === 'pg_dump' && dbs.length < 1) { e.dbs = 'pg_dump needs at least one database: untick "All databases" and pick some.'; }
        if (meth === 'mongodump' && dbs.length > 1) { e.dbs = 'mongodump takes one database or all of them.'; }
      }
    } else if (step === 2) {
      if (!S.custom && !policy()) { e.policy_choice = 'Choose a policy or Custom.'; }
      if (customSchedule()) { var se = U.validateSchedule(root); Object.keys(se).forEach(function (k) { e[k] = se[k]; }); }
      if (chk('log_archive') && !pitrSupport(S.test, meth, selectedDBs()).ok) { e.log_archive = pitrSupport(S.test, meth, selectedDBs()).reason; }
    } else if (step === 3) {
      if (!isCustomJob()) {
        if (!checkedRadio('repository')) { e.repository = D.repos.length ? 'Choose a repository.' : 'Create a repository first.'; }
        else if (v('replicate_to') && v('replicate_to') === checkedRadio('repository')) { e.replicate_to = 'The second copy must be a different repository.'; }
      }
      if (n('compress_level') < 0 || n('compress_level') > 4) { e.compress_level = 'Use 0 to 4.'; }
    } else if (step === 4) {
      if (chk('test_restore') && !scratchID()) { e.scratch_connection_id = 'Choose the connection of the scratch server.'; }
      if (testRestoreToolsMissing().length) { e.test_restore = toolMissingText(testRestoreToolsMissing()); }
      if (n('retry_max') > 0 && n('retry_backoff') < 10) { e.retry_backoff = 'Use at least 10 seconds.'; }
      if (n('retry_max') > 20) { e.retry_max = 'At most 20 retries.'; }
    }
    return e;
  }
  // Error holders of the groups that are not a single named control.
  var HOLDERS = { dbs: '#wiz-dbs-field', method: '#wiz-methods-field', repository: '#wiz-repo-field', policy_choice: '#wiz-policies', test_restore: '#wiz-tr-row' };
  var STEP_OF = { dbs: 1, method: 1, repository: 3, policy_choice: 2, test_restore: 4 };
  function placeError(k, msg) {
    var input = f(k);
    var advanced = input && input.closest && input.closest('details');
    if (advanced) { advanced.open = true; }
    if (input && input.tagName && window.strandBackupErr.field(form, k, msg)) { return true; }
    var h = HOLDERS[k] ? $(HOLDERS[k]) : null;
    if (!h) { return false; }
    var span = h.querySelector('.field-error');
    if (!span) { span = h.nextElementSibling && h.nextElementSibling.classList.contains('field-error') ? h.nextElementSibling : null; }
    if (!span) { span = el('span', 'field-error'); h.parentNode.insertBefore(span, h.nextSibling); }
    span.textContent = msg; span.hidden = false;
    return true;
  }
  function stepOf(k) {
    if (STEP_OF[k]) { return STEP_OF[k]; }
    var i = f(k);
    var h = i && i.closest ? i.closest('[data-step]') : null;
    return h ? Number(h.getAttribute('data-step')) : 0;
  }
  function showErrors(errs) {
    window.strandBackupErr.clear(form);
    var first = '';
    Object.keys(errs).forEach(function (k) { placeError(k, errs[k]); first = first || k; });
    return first;
  }

  // ---- navigation ----
  function go(step) {
    step = Math.max(1, Math.min(STEPS, step));
    S.step = step; S.reached = Math.max(S.reached, step);
    $$('.wiz-step').forEach(function (fs) { fs.hidden = Number(fs.getAttribute('data-step')) !== step; });
    $$('[data-step-li]').forEach(function (li) {
      var i = Number(li.getAttribute('data-step-li'));
      li.classList.toggle('current', i === step);
      li.classList.toggle('done', !!S.done[i] && i !== step);
      if (i === step) { li.setAttribute('aria-current', 'step'); } else { li.removeAttribute('aria-current'); }
    });
    $('#wiz-back').disabled = step === 1;
    $('#wiz-next').hidden = step === STEPS;
    $('#wiz-create-only').hidden = step !== STEPS; $('#wiz-create-run').hidden = step !== STEPS;
    syncAll();
    return step;
  }
  function tryAdvance(to) {
    // Moving forward validates every step on the way; the first invalid one is shown.
    for (var s = S.step; s < to; s++) {
      var errs = validateStep(s);
      if (Object.keys(errs).length) { S.done[s] = false; go(s); showErrors(errs); focusFirstError(); return false; }
      S.done[s] = true;
    }
    go(to);
    return true;
  }
  function focusFirstError() {
    var inv = form.querySelector('[aria-invalid="true"], .field-error:not([hidden])');
    if (inv) { var t = inv.closest('.field') || inv; if (t.scrollIntoView) { t.scrollIntoView({ block: 'center' }); } if (inv.focus && inv.tagName !== 'SPAN') { inv.focus(); } }
  }

  // ---- summary / review ----
  function scheduleText() {
    var p = policy();
    if (!customSchedule() && p) { return p.schedule_human; }
    var s = U.readSchedule(root), tz = s.timezone || 'UTC';
    if (s.kind === 'interval') { return 'Every ' + Math.round((s.every_seconds || 0) / 60) + ' min'; }
    if (s.kind === 'cron') { return 'Cron ' + (s.cron || '?') + ' ' + tz; }
    return s.kind.charAt(0).toUpperCase() + s.kind.slice(1) + ' ' + (s.at || '') + (s.days && s.days.length ? ' on ' + s.days.join(',') : '') + ' ' + tz;
  }
  function rows() {
    var c = conn(), p = policy(), out = [];
    out.push(['Name', v('name') || '-']);
    if (isCustomJob()) {
      out.push(['Source', 'Custom script ' + (v('command') || '-')]);
    } else {
      out.push(['Connection', c ? c.name + ' (' + c.engine + ', ' + c.host + (c.port ? ':' + c.port : '') + ')' : '-']);
      out.push(['Databases', S.dbsAll || !S.dbs.length ? (method() === 'pg_basebackup' ? 'Whole cluster' : 'All databases') : S.dbs.join(', ')]);
      out.push(['Method', method() || '-']);
    }
    out.push(['Policy', p ? p.name : 'Custom']);
    out.push(['Schedule', scheduleText()]);
    out.push(['Retention', customSchedule() || !p ? U.retentionWords(lockedRetention()) : p.retention_words + (n('lock_days') ? ', locked ' + n('lock_days') + ' days' : '')]);
    if (!isCustomJob()) {
      out.push(['Point-in-time recovery', chk('log_archive') ? 'On' : 'Off']);
      var rp = repoById(checkedRadio('repository'));
      out.push(['Repository', rp ? (rp.name || rp.id) + (rp.lock_mode === 'COMPLIANCE' ? ' (immutable)' : '') : '-']);
      if (v('replicate_to')) { var r2 = repoById(v('replicate_to')); out.push(['Second copy', r2 ? r2.name || r2.id : v('replicate_to')]); }
    }
    out.push(['Encryption', effectiveEncryption() ? 'On' : 'Off']);
    out.push(['Compression', chk('compress') ? 'On' : 'Off']);
    var ve = n('verify_every');
    out.push(['Verify', ve ? (ve === 86400 ? 'Daily' : ve === 604800 ? 'Weekly' : ve === 2592000 ? 'Monthly' : 'Every ' + Math.round(ve / 60) + ' min') + (chk('test_restore') ? ' + test restore' : '') : 'Never']);
    out.push(['Retries', n('retry_max') ? n('retry_max') + ' with ' + n('retry_backoff') + ' s back-off' : 'None']);
    var ov = overrides();
    if (ov.length) { out.push(['Overrides', ov.join(', ')]); }
    return out;
  }
  function fillDl(dl, list) {
    dl.textContent = '';
    list.forEach(function (r) { dl.appendChild(el('dt', '', r[0])); dl.appendChild(el('dd', '', r[1])); });
  }
  function updateSummary() { var r = rows(); fillDl($('#wiz-summary'), r); if (S.step === STEPS) { fillDl($('#wiz-review'), r); } }
  function syncAll() {
    syncCustom(); syncPolicyBlock(); syncPitr(); syncEncryption(); syncVerify(); syncTools(); syncBadges(); updateSummary();
    $('#wiz-legacy').hidden = !(EDIT && D.legacy);
  }

  // ---- events ----
  function touch(name) { S.touched[name] = true; }
  form.addEventListener('input', function (ev) {
    var t = ev.target, nm = t.name;
    if (nm === 'name') { S.nameTouched = true; }
    if (nm === 'verify_sample') { touch('verify_every_seconds'); }
    if (nm === 'retry_backoff') { touch('retry_max'); }
    syncAll();
  });
  form.addEventListener('change', function (ev) {
    var t = ev.target, nm = t.name;
    if (nm === 'connection_id') { onConnChange(); return; }
    if (nm === 'method') { S.methodTouched = true; S.methodPref = t.value; if (!S.nameTouched) { setv('name', suggestName()); } renderDbs(); syncAll(); return; }
    if (nm === 'db') { S.dbs = $$('input[name="db"]').filter(function (c) { return c.checked; }).map(function (c) { return c.value; }); if (!S.nameTouched) { setv('name', suggestName()); } syncAll(); return; }
    if (t.id === 'wiz-db-all') { S.dbsAll = t.checked; if (t.checked) { S.dbs = []; } renderDbs(); if (!S.nameTouched) { setv('name', suggestName()); } syncAll(); return; }
    if (nm === 'policy_choice') { onPolicyChoice(t.value); return; }
    if (nm === 'use_custom') { renderMethods(); renderDbs(); syncAll(); return; }
    if (nm === 'customize') { if (!t.checked) { var p = policy(); if (p) { U.writeSchedule(root, p.schedule); setv('window_minutes', p.window_minutes || 0); setv('jitter_seconds', p.jitter_seconds || 0); U.writeRetention(root, p.retention); } } syncAll(); return; }
    if (nm === 'log_archive') { touch('log_archive'); renderMethods(); }
    if (nm === 'repository') { touch('repository'); }
    if (nm === 'replicate_to' || nm === 'compress' || nm === 'test_restore' || nm === 'retry_max') { touch(nm); }
    if (nm === 'verify_every') { touch('verify_every_seconds'); }
    syncAll();
  });
  U.bindSchedule(root, function () { updateSummary(); });
  U.bindRetention(root, function () { updateSummary(); });
  $('#wiz-test-btn').addEventListener('click', function () { var id = v('connection_id'); if (id) { runTest(id, this); } });
  $('#wiz-back').addEventListener('click', function () { go(S.step - 1); });
  $('#wiz-next').addEventListener('click', function () { tryAdvance(S.step + 1); });
  $$('[data-goto]').forEach(function (b) { b.addEventListener('click', function () {
    var to = Number(b.getAttribute('data-goto'));
    if (to <= S.step) { go(to); } else { tryAdvance(to); }
  }); });
  form.addEventListener('keydown', function (ev) { if (ev.key === 'Enter' && ev.target.tagName === 'INPUT' && ev.target.type !== 'submit') { ev.preventDefault(); } });
  // A connection created in the drawer joins the list and is tested at once.
  document.addEventListener('strand:conn-saved', function (ev) {
    var c = ev.detail.connection;
    D.connections.push({ id: c.id, name: c.name, engine: c.engine, host: c.host, port: c.port, secret_mode: c.secret_mode, status: 'untested' });
    buildConnSelect(); setv('connection_id', c.id); onConnChange();
    runTest(c.id, $('#wiz-test-btn'));
  });
  var convert = $('#wiz-convert-btn');
  if (convert) { convert.addEventListener('click', function () {
    U.busy(convert, true);
    api('POST', '/api/v1/backup-jobs/' + encodeURIComponent(JOB.id) + '/convert-connection').then(function (r) { return U.readJSON(r, 'Could not convert'); })
      .then(function () { form.removeAttribute('data-dirty'); location.reload(); })
      .catch(function (e) { var a = $('#wiz-error'); a.textContent = e.message; a.hidden = false; U.busy(convert, false); });
  }); }
  window.addEventListener('beforeunload', function (ev) {
    if (form.hasAttribute('data-dirty') && !S.submitting) { ev.preventDefault(); ev.returnValue = ''; }
  });

  // ---- submit ----
  var WIZ_ERR = [[/encryption|active key/i, 'encryption_mode'], [/database/i, 'dbs'], [/connection/i, 'connection_id'], [/method/i, 'method'], [/repository|bucket/i, 'repository'], [/schedule|cron|interval|time zone|timezone/i, 'sch_kind'],
    [/retention|keep/i, 'ret_daily'], [/scratch|test.?restore/i, 'scratch_connection_id'], [/pitr|binlog|log archiv|gtid/i, 'log_archive'], [/retry|backoff/i, 'retry_max'],
    [/command/i, 'command'], [/artifact/i, 'artifact_template'], [/manifest/i, 'manifest_template'], [/job name|name/i, 'name']];
  function failSubmit(e) {
    var alertEl = $('#wiz-error'), msg = e.message || 'Request failed';
    var field = '';
    for (var i = 0; i < WIZ_ERR.length && !field; i++) { if (WIZ_ERR[i][0].test(msg)) { field = WIZ_ERR[i][1]; } }
    var placed = false;
    if (field) {
      var st = stepOf(field);
      if (st) { go(st); }
      window.strandBackupErr.clear(form);
      placed = placeError(field, msg);
    }
    alertEl.textContent = placed ? '' : msg; alertEl.hidden = placed;
    $('#wiz-done').hidden = true;
    focusFirstError();
  }
  function submit(runNow) {
    if (S.submitting) { return; }
    for (var s = 1; s <= STEPS; s++) {
      var errs = validateStep(s);
      if (Object.keys(errs).length) { go(s); showErrors(errs); focusFirstError(); return; }
    }
    var btns = [$('#wiz-create-only'), $('#wiz-create-run')], btn = runNow ? btns[1] : btns[0];
    S.submitting = true; btns.forEach(function (b) { b.disabled = true; }); U.busy(btn, true);
    $('#wiz-error').hidden = true; window.strandBackupErr.clear(form);
    var body = buildPayload();
    if (!isCustomJob() && effectiveEncryption() && !D.keys_present) {
      if (runNow) {
        S.submitting = false; btns.forEach(function (b) { b.disabled = false; }); U.busy(btn, false);
        go(3); showErrors({encryption_mode: 'Configure an active encryption key before running this job.'});
        return;
      }
      if (!EDIT || !JOB.enabled) { body.enabled = false; }
    }
    var url = '/api/v1/backup-jobs' + (EDIT ? '/' + encodeURIComponent(JOB.id) : '');
    api(EDIT ? 'PUT' : 'POST', url, body).then(function (r) { return U.readJSON(r, 'Could not save the job'); }).then(function (res) {
      var id = res.job.id; S.savedJobID = id;
      form.removeAttribute('data-dirty');
      var next = runNow ? api('POST', '/api/v1/backup-jobs/' + encodeURIComponent(id) + '/run').then(function (r) { return U.readJSON(r, 'The job was saved but the first run could not start'); }) : Promise.resolve();
      return next.then(function () { return res; });
    }).then(function (res) {
      var done = $('#wiz-done'); done.textContent = ''; done.hidden = false;
      done.appendChild(el('b', '', EDIT ? 'Job saved.' : 'Job created.'));
      done.appendChild(document.createTextNode(runNow ? ' The first backup is starting. ' : ' '));
      var a = el('a', 'btn small primary', 'Open the job'); a.href = '/backups/jobs/' + encodeURIComponent(S.savedJobID); done.appendChild(a);
      var w = $('#wiz-warnings'); w.textContent = ''; w.hidden = !(res.warnings && res.warnings.length);
      (res.warnings || []).forEach(function (m) { w.appendChild(el('p', '', m)); });
      if (!(res.warnings && res.warnings.length)) { setTimeout(function () { location.href = a.getAttribute('href'); }, 1200); }
      btns.forEach(function (b) { b.hidden = true; });
    }).catch(function (e) {
      S.submitting = false; btns.forEach(function (b) { b.disabled = false; });
      if (S.savedJobID && runNow) { var a = $('#wiz-error'); a.textContent = e.message + ' Open the job to run it manually.'; a.hidden = false; return; }
      failSubmit(e);
    }).then(function () { U.busy(btn, false); });
  }
  $('#wiz-create-only').addEventListener('click', function () { submit(false); });
  $('#wiz-create-run').addEventListener('click', function () { submit(true); });

  // ---- init ----
  function init() {
    buildConnSelect(); renderRepos(); renderPolicies();
    if (EDIT && JOB) { loadJob(); }
    else {
      var def = policyById('policy-silver') || D.policies[0];
      if (def) { S.policy = def.id; S.custom = false; } else { S.custom = true; }
      if (S.custom) { applyCustomDefaults(); } else { applyPolicyDefaults(); }
      setRadio('policy_choice', S.custom ? '__custom' : S.policy);
      U.writeSchedule(root, S.custom ? { kind: 'daily', at: '01:00' } : policy().schedule);
      if (S.custom) { U.writeRetention(root, { keep_daily: 7, keep_weekly: 4 }); }
      f('sch_tz').value = f('sch_tz').value || (function () { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ''; } catch (e) { return ''; } }());
    }
    syncEncryption();
    go(1);
    form.removeAttribute('data-dirty');
    if (EDIT && JOB && JOB.connection_id && connById(JOB.connection_id)) {
      var b = $('#wiz-test-btn'); b.disabled = false;
      runTest(JOB.connection_id, b);
    }
  }
  function loadJob() {
    setv('name', JOB.name); S.nameTouched = true;
    if (JOB.method === 'custom' || !JOB.method) {
      setc('use_custom', true);
      ['command', 'artifact_template', 'manifest_template', 'bucket', 'prefix'].forEach(function (k) { setv(k, JOB[k]); });
      $('#wiz-custom').open = true;
    } else {
      setv('connection_id', JOB.connection_id || '');
      S.methodPref = JOB.method; S.methodTouched = true;
      var dbs = (JOB.conn && JOB.conn.databases) || [];
      S.dbs = dbs.slice(); S.dbsAll = dbs.length === 0;
      $('#wiz-test-btn').disabled = !JOB.connection_id;
    }
    var p = JOB.policy_id ? policyById(JOB.policy_id) : null;
    S.policy = p ? p.id : ''; S.custom = !p;
    renderPolicies(); renderMethods(); renderDbs();
    var ov = JOB.overrides || [];
    S.touched = {};
    ov.forEach(function (k) { S.touched[k] = true; });
    var sched = JOB.schedule || { kind: 'interval', every_seconds: JOB.every_seconds };
    U.writeSchedule(root, sched); setv('window_minutes', JOB.window_minutes || 0); setv('jitter_seconds', JOB.jitter_seconds || 0);
    U.writeRetention(root, JOB.retention || {}); setv('lock_days', (JOB.retention && JOB.retention.lock_days) || 0);
    setc('customize', !!p && (ov.indexOf('schedule') >= 0 || ov.indexOf('retention') >= 0 || ov.indexOf('window_minutes') >= 0 || ov.indexOf('jitter_seconds') >= 0));
    setc('log_archive', JOB.log_archive); setc('compress', JOB.compress); setv('encryption_mode', p && ov.indexOf('encrypt') < 0 ? 'inherit' : JOB.encrypt ? 'on' : 'off');
    setv('compress_level', JOB.compress_level || 0); setv('bandwidth_kbps', JOB.bandwidth_kbps || 0);
    setRepo(JOB.repository || ''); setv('replicate_to', JOB.replicate_to || '');
    setv('verify_every', JOB.verify_every_seconds || 0);
    if (JOB.verify_every_seconds && !$$('option', f('verify_every')).some(function (o) { return Number(o.value) === JOB.verify_every_seconds; })) {
      var o = el('option', '', 'Every ' + Math.round(JOB.verify_every_seconds / 60) + ' min'); o.value = JOB.verify_every_seconds; f('verify_every').appendChild(o); f('verify_every').value = String(JOB.verify_every_seconds);
    }
    setv('verify_sample', JOB.verify_sample_percent || 100); setc('test_restore', JOB.test_restore);
    setv('retry_max', JOB.retry_max || 0); setv('retry_backoff', JOB.retry_backoff_seconds || 60);
    if (JOB.test_restore) {
      var sc = JOB.scratch_connection_id ? connById(JOB.scratch_connection_id)
        : (JOB.scratch ? D.connections.filter(function (c) { return c.host === JOB.scratch.host; })[0] : null);
      if (sc) { setv('scratch_connection_id', sc.id); }
    }
    for (var i = 1; i <= STEPS; i++) { S.done[i] = true; }
    S.reached = STEPS;
  }
  init();
}());
