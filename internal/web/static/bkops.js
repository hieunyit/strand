// Backup repositories, notification channels and rules: forms and buttons of /backups/repositories*,
// /backups/notifications. Every write goes through window.strandApi (session cookie + CSRF header); the server
// enforces RBAC. Secrets typed here are sent once and never rendered back.
(function () {
  'use strict';
  if (!window.strandApi || !document.querySelector('[data-bkops]')) { return; }
  var api = window.strandApi;

  function $(id) { return document.getElementById(id); }
  function json(resp, fallback) {
    return resp.json().catch(function () { return {}; }).then(function (body) {
      if (!resp.ok) {
        var msg = body.error || fallback;
        ['jobs', 'policies'].forEach(function (k) { if (body[k] && body[k].length) { msg += ' (' + k + ': ' + body[k].join(', ') + ')'; } });
        throw new Error(msg);
      }
      return body;
    });
  }
  function say(el, text, bad) {
    if (!el) { return; }
    el.textContent = text || '';
    el.classList.toggle('err', !!bad);
  }
  function val(f, name) { var c = f.elements.namedItem(name); return c ? String(c.value || '').trim() : ''; }

  // data-bk-show="name=a|b" shows the element when the control's value is one of the listed values;
  // data-bk-show="name" when it is non-empty.
  function syncShow(f) {
    f.querySelectorAll('[data-bk-show]').forEach(function (el) {
      var cond = el.getAttribute('data-bk-show'), eq = cond.indexOf('='), on;
      var name = eq < 0 ? cond : cond.slice(0, eq);
      var c = f.elements.namedItem(name);
      var v = '';
      if (c && c.length !== undefined && !c.tagName) { // RadioNodeList
        v = c.value;
      } else if (c) { v = String(c.value || ''); }
      on = eq < 0 ? v !== '' : cond.slice(eq + 1).split('|').indexOf(v) >= 0;
      el.hidden = !on;
    });
  }
  function wireDrawer(drawerId, formId, openId, closeIds) {
    var d = $(drawerId), f = $(formId);
    if (!d || !f) { return; }
    var open = $(openId);
    if (open) { open.addEventListener('click', function () { d.hidden = false; syncShow(f); var first = f.querySelector('input'); if (first) { first.focus(); } }); }
    closeIds.forEach(function (id) { var b = $(id); if (b) { b.addEventListener('click', function () { d.hidden = true; f.reset(); syncShow(f); }); } });
    f.addEventListener('input', syncShow.bind(null, f));
    f.addEventListener('change', syncShow.bind(null, f));
    syncShow(f);
  }

  // ---- repositories ----
  // A repository is defined entirely here: endpoint, region, keys (sealed by the server), bucket, path, default
  // retention and Object Lock. Environment-variable repositories are legacy: an edit converts them.
  var repoEdit = null;
  var RET_FIELDS = ['max_age_days', 'keep_last', 'keep_within_days', 'keep_daily', 'keep_weekly', 'keep_monthly', 'keep_yearly'];
  var PROVIDERS = {
    aws: { ph: 'https://s3.<region>.amazonaws.com', region: 'eu-west-1' },
    minio: { ph: 'https://minio.example.com:9000', region: 'us-east-1' },
    ceph: { ph: 'https://rgw.example.com', region: 'default' },
    wasabi: { ph: 'https://s3.<region>.wasabisys.com', region: 'us-east-1' },
    other: { ph: 'https://s3.example.com', region: '' }
  };
  var repoForm = $('bk-repo-form');
  var autoEndpoint = '';
  function setField(f, n, v) {
    var c = f.elements.namedItem(n);
    if (!c || !c.tagName) { return; }
    if (c.type === 'checkbox') { c.checked = !!v; } else { c.value = v === undefined || v === null ? '' : v; }
  }
  function num(f, n) { var x = Number(val(f, n)); return isFinite(x) && x > 0 ? Math.floor(x) : 0; }
  function repoProviderSync() {
    if (!repoForm) { return; }
    var pv = PROVIDERS[val(repoForm, 'provider')] || PROVIDERS.other;
    var ep = repoForm.elements.namedItem('endpoint'), rg = repoForm.elements.namedItem('region');
    if (ep) { ep.placeholder = pv.ph; }
    if (rg) { rg.placeholder = pv.region || 'optional'; }
    var region = val(repoForm, 'region');
    var tmpl = pv.ph.indexOf('<region>') >= 0 && region ? pv.ph.replace('<region>', region) : '';
    // Fill the endpoint from the provider format while the user has not typed one of their own.
    if (ep && tmpl && (ep.value === '' || ep.value === autoEndpoint)) { ep.value = tmpl; autoEndpoint = tmpl; }
  }
  function repoPathPreview() {
    var out = $('bk-repo-path-preview');
    if (!out || !repoForm) { return; }
    var b = val(repoForm, 'bucket') || 'bucket', p = val(repoForm, 'prefix').replace(/^\/+|\/+$/g, '');
    out.textContent = 's3://' + b + '/' + (p ? p + '/' : '') + '<source>/...';
  }
  function repoReset() {
    repoEdit = null; autoEndpoint = '';
    var t = $('bk-repo-title'); if (t) { t.textContent = 'New repository'; }
    var lg = $('bk-repo-legacy'); if (lg) { lg.hidden = true; }
    var dr = $('bk-repo-draft-result'); if (dr) { dr.hidden = true; }
    say($('bk-repo-message'), '');
    if (!repoForm) { return; }
    ['access_key', 'secret_key'].forEach(function (n) { var c = repoForm.elements.namedItem(n); if (c) { c.placeholder = ''; c.required = true; } });
    var sb = repoForm.querySelector('button[type="submit"]'); if (sb) { sb.textContent = 'Add repository'; }
    if (window.history && window.location.search.indexOf('edit=') >= 0) { window.history.replaceState(null, '', '/backups/repositories'); }
  }
  wireDrawer('bk-repo-drawer', 'bk-repo-form', 'bk-repo-new', ['bk-repo-close', 'bk-repo-cancel']);
  ['bk-repo-new', 'bk-repo-close', 'bk-repo-cancel'].forEach(function (bid) {
    var b = $(bid); if (b) { b.addEventListener('click', function () { repoReset(); repoProviderSync(); repoPathPreview(); }); }
  });
  if (repoForm) {
    repoForm.addEventListener('input', function () { repoProviderSync(); repoPathPreview(); });
    repoForm.addEventListener('change', function () { repoProviderSync(); repoPathPreview(); });
    repoProviderSync(); repoPathPreview();
  }
  // /backups/repositories?edit=<id> opens the drawer on that repository (the detail page links here).
  var editID = new URLSearchParams(window.location.search).get('edit');
  if (repoForm && editID) {
    api('GET', '/api/v1/backup-repositories/' + encodeURIComponent(editID)).then(function (r) { return json(r, 'Could not load the repository'); }).then(function (b) {
      var rp = b.repository || b;
      repoEdit = rp;
      ['name', 'bucket', 'prefix', 'endpoint', 'region', 'lock_mode', 'lock_days', 'allow_insecure_local'].forEach(function (n) { setField(repoForm, n, rp[n]); });
      setField(repoForm, 'kind', rp.kind || 's3');
      setField(repoForm, 'provider', /amazonaws\.com/.test(rp.endpoint || '') ? 'aws' : (/wasabisys\.com/.test(rp.endpoint || '') ? 'wasabi' : 'other'));
      if (!rp.lock_mode) { setField(repoForm, 'lock_days', 30); }
      var ret = rp.retention || {};
      RET_FIELDS.forEach(function (k) { setField(repoForm, 'ret_' + k, ret[k] || 0); });
      var stored = rp.secret_mode === 'stored';
      [['access_key', stored && rp.access_key_set], ['secret_key', stored && rp.secret_key_set]].forEach(function (p) {
        var c = repoForm.elements.namedItem(p[0]); if (c) { c.value = ''; c.required = !p[1]; c.placeholder = p[1] ? 'Stored; leave blank to keep' : ''; }
      });
      var lg = $('bk-repo-legacy');
      if (lg && !stored) { lg.hidden = false; var px = $('bk-repo-legacy-prefix'); if (px) { px.textContent = (rp.env_prefix || '') + '*'; } }
      var title = $('bk-repo-title'); if (title) { title.textContent = 'Edit repository ' + (rp.name || rp.id); }
      var btn = repoForm.querySelector('button[type="submit"]'); if (btn) { btn.textContent = 'Save changes'; }
      $('bk-repo-drawer').hidden = false; syncShow(repoForm); repoPathPreview();
      var first = repoForm.elements.namedItem('name'); if (first) { first.focus(); }
    }).catch(function (e) { say($('bk-repo-message') || document.body, e.message, true); });
  }
  // repoBody builds the request from the form; it returns null (and says why) when a required field is empty.
  function repoBody(forTest) {
    var lockMode = val(repoForm, 'lock_mode');
    var body = { name: val(repoForm, 'name'), kind: val(repoForm, 'kind') || 's3', secret_mode: 'stored',
      endpoint: val(repoForm, 'endpoint'), region: val(repoForm, 'region'),
      allow_insecure_local: !!(repoForm.elements.namedItem('allow_insecure_local') || {}).checked,
      bucket: val(repoForm, 'bucket'), prefix: val(repoForm, 'prefix').replace(/^\/+|\/+$/g, ''),
      lock_mode: lockMode, lock_days: lockMode ? num(repoForm, 'lock_days') : 0, retention: {} };
    RET_FIELDS.forEach(function (k) { var v = num(repoForm, 'ret_' + k); if (v) { body.retention[k] = v; } });
    if (repoEdit && repoEdit.retention && repoEdit.retention.keep_hourly) { body.retention.keep_hourly = repoEdit.retention.keep_hourly; }
    var keysStored = repoEdit && repoEdit.secret_mode === 'stored';
    var ak = val(repoForm, 'access_key'), sk = val(repoForm, 'secret_key');
    // Editing a stored repository: a blank key keeps the stored (sealed) key; the server never returns it.
    if (!keysStored || ak) { body.access_key = ak; }
    if (!keysStored || sk) { body.secret_key = sk; }
    var missing = [];
    if (!forTest && !body.name) { missing.push('name'); }
    if (!body.endpoint) { missing.push('endpoint URL'); }
    if (!body.bucket) { missing.push('bucket'); }
    if (!keysStored && !ak) { missing.push('access key ID'); }
    if (!keysStored && !sk) { missing.push('secret access key'); }
    if (lockMode && !body.lock_days) { missing.push('lock period'); }
    if (missing.length) { say($('bk-repo-message'), 'Required: ' + missing.join(', '), true); return null; }
    if (forTest && repoEdit) { body.id = repoEdit.id; }
    return body;
  }
  function renderSteps(table, steps) {
    var tbody = table.querySelector('tbody');
    while (tbody.firstChild) { tbody.removeChild(tbody.firstChild); }
    (steps || []).forEach(function (s) {
      var tr = document.createElement('tr');
      [s.name, null, s.detail || '', (s.latency_ms || 0) + ' ms'].forEach(function (text, i) {
        var td = document.createElement('td');
        if (i === 1) {
          var b = document.createElement('span');
          b.className = 'badge ' + (s.status === 'ok' ? 'st-synced' : (s.status === 'failed' ? 'st-failed' : ''));
          b.textContent = s.status; td.appendChild(b);
        } else { td.textContent = text; if (i === 2) { td.className = 'small'; } }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.hidden = false;
  }
  var draftBtn = $('bk-repo-draft-test');
  if (repoForm && draftBtn) {
    draftBtn.addEventListener('click', function () {
      var body = repoBody(true);
      if (!body) { return; }
      say($('bk-repo-message'), 'Testing the connection...');
      draftBtn.disabled = true;
      api('POST', '/api/v1/backup-repositories/test', body).then(function (r) { return json(r, 'Test failed'); })
        .then(function (res) {
          var box = $('bk-repo-draft-result'); if (box) { box.hidden = false; }
          renderSteps($('bk-repo-draft-steps'), res.steps);
          say($('bk-repo-message'), res.ok ? 'Connection OK: all steps passed (' + res.latency_ms + ' ms)' : 'The connection test failed; check the steps below', !res.ok);
        })
        .catch(function (e) { say($('bk-repo-message'), e.message, true); })
        .then(function () { draftBtn.disabled = false; });
    });
  }
  if (repoForm) {
    repoForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var body = repoBody(false);
      if (!body) { return; }
      var url = '/api/v1/backup-repositories' + (repoEdit ? '/' + encodeURIComponent(repoEdit.id) : '');
      api(repoEdit ? 'PUT' : 'POST', url, body).then(function (r) { return json(r, 'Could not save the repository'); })
        .then(function (b) { window.location.href = '/backups/repositories/' + encodeURIComponent(b.repository.id); })
        .catch(function (e) { say($('bk-repo-message'), e.message, true); });
    });
  }
  var detail = document.querySelector('[data-bkops="repo-detail"]');
  if (detail) {
    var rid = detail.getAttribute('data-repo-id');
    var testBtn = $('bk-repo-test');
    if (testBtn) {
      testBtn.addEventListener('click', function () {
        var st = $('bk-repo-test-status');
        say(st, 'Testing...');
        testBtn.disabled = true;
        api('POST', '/api/v1/backup-repositories/' + encodeURIComponent(rid) + '/test').then(function (r) { return json(r, 'Test failed'); })
          .then(function (res) {
            renderSteps($('bk-repo-steps'), res.steps);
            var none = $('bk-repo-nosteps'); if (none) { none.hidden = true; }
            say(st, res.ok ? 'All steps passed (' + res.latency_ms + ' ms)' : 'The test failed', !res.ok);
          })
          .catch(function (e) { say(st, e.message, true); })
          .then(function () { testBtn.disabled = false; });
      });
    }
    var delBtn = $('bk-repo-delete');
    if (delBtn) {
      delBtn.addEventListener('click', function () {
        window.strandConfirm('Delete repository ' + rid + '? Its stored credentials are removed. This is refused while it holds backups or a job or policy uses it.', rid, { ok: 'Delete repository' }).then(function (yes) {
          if (!yes) { return; }
          api('DELETE', '/api/v1/backup-repositories/' + encodeURIComponent(rid)).then(function (r) {
            if (r.status === 204) { window.location.href = '/backups/repositories'; return null; }
            return json(r, 'Could not delete the repository');
          }).catch(function (e) { say($('bk-repo-test-status'), e.message, true); });
        });
      });
    }
  }

  // ---- notification channels and rules ----
  var chanEdit = null;
  wireDrawer('bk-chan-drawer', 'bk-chan-form', 'bk-chan-new', ['bk-chan-close', 'bk-chan-cancel']);
  ['bk-chan-new', 'bk-chan-close', 'bk-chan-cancel'].forEach(function (bid) {
    var b = $(bid); if (!b) { return; }
    b.addEventListener('click', function () {
      chanEdit = null;
      var t = $('bk-chan-title'); if (t) { t.textContent = 'New channel'; }
      var f = $('bk-chan-form'); if (!f) { return; }
      var k = f.elements.namedItem('kind'); if (k) { k.disabled = false; }
      var u = f.elements.namedItem('url'); if (u) { u.placeholder = ''; }
      var sb = f.querySelector('button[type="submit"]'); if (sb) { sb.textContent = 'Add channel'; }
    });
  });
  var chanForm = $('bk-chan-form');
  function openChannelEdit(id) {
    api('GET', '/api/v1/backup-notify-channels/' + encodeURIComponent(id)).then(function (r) { return json(r, 'Could not load the channel'); }).then(function (b) {
      var c = b.channel || b;
      chanEdit = c;
      var set = function (n, v) { var x = chanForm.elements.namedItem(n); if (x && x.tagName) { x.value = v === undefined || v === null ? '' : v; } };
      set('name', c.name); set('kind', c.kind); set('smtp_addr', c.smtp_addr); set('from', c.from); set('user', c.user);
      set('to', (c.to || []).join(', ')); set('url', ''); set('password', '');
      var kindSel = chanForm.elements.namedItem('kind'); if (kindSel) { kindSel.disabled = true; } // the sealed secret belongs to this kind
      var u = chanForm.elements.namedItem('url'); if (u) { u.required = false; u.placeholder = 'Set; leave blank to keep'; }
      var title = $('bk-chan-title'); if (title) { title.textContent = 'Edit channel ' + (c.name || c.id); }
      var btn = chanForm.querySelector('button[type="submit"]'); if (btn) { btn.textContent = 'Save changes'; }
      $('bk-chan-drawer').hidden = false; syncShow(chanForm);
    }).catch(function (e) { say($('bk-chan-result'), e.message, true); });
  }
  if (chanForm) {
    chanForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var kind = val(chanForm, 'kind');
      var body = { name: val(chanForm, 'name'), kind: kind };
      if (kind === 'email') {
        body.smtp_addr = val(chanForm, 'smtp_addr'); body.from = val(chanForm, 'from'); body.user = val(chanForm, 'user');
        body.to = val(chanForm, 'to').split(/[,\s]+/).filter(Boolean);
        var pw = val(chanForm, 'password'); if (pw) { body.password = pw; }
      } else {
        var u = val(chanForm, 'url');
        if (!chanEdit || u) { body.url = u; } // editing: blank keeps the stored URL
      }
      var curl = '/api/v1/backup-notify-channels' + (chanEdit ? '/' + encodeURIComponent(chanEdit.id) : '');
      api(chanEdit ? 'PUT' : 'POST', curl, body).then(function (r) { return json(r, 'Could not save the channel'); })
        .then(function () { window.location.reload(); })
        .catch(function (e) { say($('bk-chan-message'), e.message, true); });
    });
  }
  document.addEventListener('click', function (ev) {
    var t = ev.target.closest ? ev.target.closest('button') : null;
    if (!t) { return; }
    var id;
    if ((id = t.getAttribute('data-bk-chan-test'))) {
      var out = $('bk-chan-result');
      say(out, 'Sending...');
      api('POST', '/api/v1/backup-notify-channels/' + encodeURIComponent(id) + '/test').then(function (r) { return json(r, 'Test failed'); })
        .then(function (res) { say(out, res.ok ? 'Test delivered.' : 'Delivery failed: ' + res.detail, !res.ok); })
        .catch(function (e) { say(out, e.message, true); });
    } else if ((id = t.getAttribute('data-bk-chan-delete'))) {
      var cid = id;
      window.strandConfirm('Delete channel ' + cid + ' and the rules that use it?', cid, { ok: 'Delete channel' }).then(function (yes) {
        if (!yes) { return; }
        api('DELETE', '/api/v1/backup-notify-channels/' + encodeURIComponent(cid)).then(function (r) { return r.status === 204 ? null : json(r, 'Could not delete'); })
          .then(function () { window.location.reload(); }).catch(function (e) { say($('bk-chan-result'), e.message, true); });
      });
    } else if ((id = t.getAttribute('data-bk-chan-edit'))) {
      openChannelEdit(id);
    } else if ((id = t.getAttribute('data-bk-rule-toggle'))) {
      var on = t.getAttribute('data-enabled') === '1';
      api('PUT', '/api/v1/backup-notify-rules/' + encodeURIComponent(id), { enabled: !on }).then(function (r) { return json(r, 'Could not update'); })
        .then(function () { window.location.reload(); }).catch(function (e) { say($('bk-rule-message'), e.message, true); });
    } else if ((id = t.getAttribute('data-bk-rule-delete'))) {
      api('DELETE', '/api/v1/backup-notify-rules/' + encodeURIComponent(id)).then(function (r) { return r.status === 204 ? null : json(r, 'Could not delete'); })
        .then(function () { window.location.reload(); }).catch(function (e) { say($('bk-rule-message'), e.message, true); });
    }
  });
  var ruleForm = $('bk-rule-form');
  if (ruleForm) {
    ruleForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var scope = val(ruleForm, 'scope').split(':');
      var body = { event: val(ruleForm, 'event'), scope_kind: scope[0], scope_id: scope.slice(1).join(':'), channel_id: val(ruleForm, 'channel_id') };
      api('POST', '/api/v1/backup-notify-rules', body).then(function (r) { return json(r, 'Could not add the rule'); })
        .then(function () { window.location.reload(); }).catch(function (e) { say($('bk-rule-message'), e.message, true); });
    });
  }
}());
