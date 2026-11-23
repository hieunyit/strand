// Strand web console: pipeline pages. (1) The New pipeline wizard: steps, per-step validation, live summary,
// Test connection. (2) The Pipelines list: row selection and bulk Start/Stop. Loaded on every page; each part
// returns at once when its page is not showing. No libraries, no inline script (CSP).
(function () {
  'use strict';
  function $(id) { return document.getElementById(id); }
  function make(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text !== undefined) { e.textContent = text; }
    return e;
  }
  function con() { return window.strandConsole || {}; }
  function toast(msg, ok) { if (con().toast) { con().toast(msg, ok); } }

  // ---------------------------------------------------------------- New pipeline wizard
  var ENGINES = { mysql: 'MySQL', mariadb: 'MariaDB', postgres: 'PostgreSQL', mongodb: 'MongoDB' };
  var OPTION_FIELDS = [['commit_order', 'relaxed'], ['workers', '8'], ['spool_dir', ''], ['iops_soft', ''], ['iops_hard', ''], ['chunk_rows', ''],
    ['max_parallel', ''], ['windows', ''], ['governor_algorithm', ''], ['lag_slo', ''], ['breaker_max_ratio', ''],
    ['replica_lag_max', ''], ['snapshot_batch', ''], ['stream_batch', ''], ['lease_ttl', '']];

  function wizard(form) {
    var steps = Array.prototype.slice.call(form.querySelectorAll('.wiz-step'));
    var inds = Array.prototype.slice.call(document.querySelectorAll('#np-stepper .step'));
    var back = $('np-back'), next = $('np-next'), create = $('btn-create-pipeline'), status = $('np-status');
    var from = $('np-from'), to = $('np-to');
    var labels = ['Source', 'Target', 'Tables & options', 'Review'];
    var cur = 1, last = steps.length, errSeq = 0;

    function field(name) { return form.querySelector('[name="' + name + '"]:not([disabled])'); }
    function val(name) { var e = field(name); return e ? e.value.trim() : ''; }

    // ---- inline field errors (label.field > .field-error) ----
    function wrapOf(f) { return f.closest('.field') || f.parentElement; }
    function clearErr(f) {
      var w = wrapOf(f);
      if (!w) { return; }
      w.classList.remove('has-error');
      var s = w.querySelector(':scope > .field-error');
      if (s) {
        var ids = (f.getAttribute('aria-describedby') || '').split(/\s+/).filter(function (id) { return id && id !== s.id; });
        if (ids.length) { f.setAttribute('aria-describedby', ids.join(' ')); }
        else { f.removeAttribute('aria-describedby'); }
        s.remove();
      }
      f.removeAttribute('aria-invalid');
    }
    function showErr(f, msg) {
      clearErr(f);
      var w = wrapOf(f);
      if (!w) { return; }
      w.classList.add('has-error');
      var s = make('small', 'field-error', msg);
      s.id = 'np-err-' + (++errSeq);
      s.setAttribute('role', 'alert');
      w.appendChild(s);
      f.setAttribute('aria-invalid', 'true');
      f.setAttribute('aria-describedby', [f.getAttribute('aria-describedby'), s.id].filter(Boolean).join(' '));
    }
    function problem(f) {
      var v = (f.value || '').trim();
      var ssh = /^(source|target)_ssh_(host|user)$/.exec(f.name);
      if (ssh && !v && window.strandPipelineSSH && window.strandPipelineSSH(form, ssh[1])) {
        var details = f.closest('details');
        if (details) { details.open = true; }
        return 'This field is required when using an SSH tunnel.';
      }
      if (f.required && v === '') { return 'This field is required.'; }
      if (v && f.pattern && !new RegExp('^(?:' + f.pattern.replace(/^\^|\$$/g, '') + ')$').test(v)) {
        return f.name === 'name' ? 'Use letters, digits and - _ . (up to 63), starting with a letter or digit.' : 'This value has an invalid format.';
      }
      if (f.type === 'number' && v !== '') {
        var n = Number(v);
        if (isNaN(n) || (f.min !== '' && n < Number(f.min)) || (f.max !== '' && n > Number(f.max))) {
          return 'Enter a number' + (f.min !== '' && f.max !== '' ? ' between ' + f.min + ' and ' + f.max : '') + '.';
        }
      }
      return '';
    }
    function checkable(f) { return !f.disabled && f.type !== 'hidden' && f.type !== 'checkbox' && !f.closest('[hidden]'); }
    function validateWithin(root) {
      var first = null;
      Array.prototype.forEach.call(root.querySelectorAll('input, select, textarea'), function (f) {
        if (!checkable(f)) { return; }
        var m = problem(f);
        if (m) { showErr(f, m); if (!first) { first = f; } } else { clearErr(f); }
      });
      if (first) { first.focus(); }
      return !first;
    }
    // Validate on blur and clear the message as soon as the value is fixed.
    form.addEventListener('focusout', function (ev) {
      var f = ev.target;
      if (!f || !f.matches || !f.matches('input, select, textarea') || !checkable(f)) { return; }
      var m = problem(f);
      if (m) { showErr(f, m); } else { clearErr(f); }
    });
    form.addEventListener('input', function (ev) {
      var f = ev.target;
      if (f && f.matches && f.matches('input, select, textarea') && wrapOf(f) && wrapOf(f).classList.contains('has-error') && !problem(f)) { clearErr(f); }
    });

    // ---- steps ----
    function goto(n) {
      cur = Math.max(1, Math.min(last, n));
      steps.forEach(function (s) { s.hidden = Number(s.getAttribute('data-step')) !== cur; });
      inds.forEach(function (li, i) {
        var k = i + 1;
        li.classList.toggle('done', k < cur);
        li.classList.toggle('current', k === cur);
        if (k === cur) { li.setAttribute('aria-current', 'step'); } else { li.removeAttribute('aria-current'); }
        var idx = li.querySelector('.step-index');
        if (idx) { idx.textContent = k < cur ? '✓' : String(k); }
      });
      back.hidden = cur === 1;
      next.hidden = cur === last;
      create.hidden = cur !== last;
      if (status) { status.textContent = 'Step ' + cur + ' of ' + last + ': ' + labels[cur - 1]; }
      render();
      var h = steps[cur - 1].querySelector('legend');
      if (h) { h.focus(); if (h.scrollIntoView) { h.scrollIntoView({ block: 'start' }); } }
    }
    function stepOf(f) { var s = f.closest('.wiz-step'); return s ? Number(s.getAttribute('data-step')) : 1; }
    next.addEventListener('click', function () { if (validateWithin(steps[cur - 1])) { goto(cur + 1); } });
    back.addEventListener('click', function () { goto(cur - 1); });
    // Enter inside a field moves on instead of submitting half a form.
    form.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' && ev.target && ev.target.tagName === 'INPUT' && ev.target.type !== 'checkbox' && ev.target.type !== 'submit' && cur < last) {
        ev.preventDefault();
        next.click();
      }
    });
    function validateAll() {
      for (var i = 0; i < steps.length; i++) {
        if (!validateWithin(steps[i])) { goto(i + 1); validateWithin(steps[i]); return false; }
      }
      return true;
    }

    // ---- summary (side column and Review) ----
    function addr(prefix, withDb) {
      var h = val(prefix + '_host'), p = val(prefix + '_port'), u = val(prefix + '_user');
      var s = (h || '?') + (p ? ':' + p : '') + (u ? ' as ' + u : '');
      if (withDb) { s += ' / ' + (withDb); }
      var bastion = val(prefix + '_ssh_host');
      if (bastion) { s += ' via SSH ' + bastion + ':' + (val(prefix + '_ssh_port') || '22'); }
      return s;
    }
    function summaryRows() {
      var src = from.value, dst = to.value, rows = [];
      rows.push(['Name', val('name')]);
      if (src === 'mongodb') {
        rows.push(['Source', 'MongoDB, ' + addr('source', val('source_database') || '?') + (val('source_replica_set') ? ' (replica set ' + val('source_replica_set') + ')' : '')]);
        rows.push(['Source password', val('source_password') ? 'set (hidden)' : 'none']);
      } else {
        rows.push(['Source', ENGINES[src] + ', ' + addr('source', src === 'postgres' ? val('database') : '')]);
        rows.push(['Source password', val('source_password') ? 'set (hidden)' : 'none']);
      }
      if (dst === 'mongodb') {
        rows.push(['Target', 'MongoDB, ' + addr('target', val('target_mongo_database') || val('source_database') || '?') + (val('target_replica_set') ? ' (replica set ' + val('target_replica_set') + ')' : '')]);
        rows.push(['Target password', val('target_password') ? 'set (hidden)' : 'none']);
      } else {
        rows.push(['Target', ENGINES[dst] + ', ' + addr('target', dst === 'postgres' ? val('target_pg_database') : '')]);
        rows.push(['Target password', val('target_password') ? 'set (hidden)' : 'none']);
      }
      if (val('start_at')) { rows.push(['CDC start position', val('start_at') + ' (from restored copy)']); }
      var pv = $('np-preview');
      rows.push(['Copy', pv ? pv.textContent : '']);
      var opts = [];
      OPTION_FIELDS.forEach(function (o) { var v = val(o[0]); if (v && v !== o[1]) { opts.push(o[0] + ' ' + v); } });
      if (val('keyless') === 'full_row') { opts.push('full-row matching for tables without a key'); }
      var autoSeed = field('auto_seed');
      if (autoSeed && autoSeed.checked) { opts.push('automatically copy tables without a usable key and their foreign-key groups'); }
      rows.push(['Options', opts.length ? opts.join(', ') : 'defaults']);
      return rows;
    }
    function render() {
      var rows = summaryRows();
      var map = { Name: 'sum-name', Source: 'sum-source', Target: 'sum-target', Copy: 'sum-copy', Options: 'sum-options' };
      rows.forEach(function (r) {
        var d = map[r[0]] && $(map[r[0]]);
        if (!d) { return; }
        d.textContent = r[1] || 'not set';
        d.classList.toggle('is-empty', !r[1]);
      });
      var rv = $('np-review');
      if (rv && cur === last) {
        rv.textContent = '';
        rows.forEach(function (r) {
          rv.appendChild(make('dt', '', r[0]));
          rv.appendChild(make('dd', r[1] ? '' : 'is-empty', r[1] || 'not set'));
        });
      }
    }
    form.addEventListener('input', render);
    form.addEventListener('change', render);

    // ---- Test connection ----
    function testConn(role) {
      var out = $('np-test-' + role + '-result'), btn = $('np-test-' + role);
      var eng = role === 'source' ? from.value : to.value, pre = role === 'source' ? 'source' : 'target';
      var box = btn.closest('fieldset');
      // only the connection fields of this side are checked here
      var names = eng === 'mongodb' ? [pre + '_host', pre + '_port'] : [pre + '_host', pre + '_port', pre + '_user'];
      var ssh = window.strandPipelineSSH && window.strandPipelineSSH(form, pre);
      if (ssh) { names = names.concat([pre + '_ssh_host', pre + '_ssh_port', pre + '_ssh_user']); }
      var bad = null;
      names.forEach(function (n) {
        var f = field(n);
        if (!f) { return; }
        var m = problem(f);
        if (m) { showErr(f, m); if (!bad) { bad = f; } } else { clearErr(f); }
      });
      if (bad) { bad.focus(); return; }
      var body = { role: role, engine: eng };
      body.host = val(pre + '_host'); body.port = Number(val(pre + '_port')); body.user = val(pre + '_user'); body.password = field(pre + '_password') ? field(pre + '_password').value : '';
      if (ssh) { body.ssh_tunnel = ssh; }
      if (eng === 'mongodb') {
        body.auth_db = val(pre + '_auth_db'); body.replica_set = val(pre + '_replica_set');
        body.tls = !!(field(pre + '_tls') && field(pre + '_tls').checked);
      }
      out.textContent = '';
      var sk = make('div', 'skeleton-rows'); sk.appendChild(make('div', 'skeleton')); sk.appendChild(make('div', 'skeleton'));
      sk.setAttribute('aria-label', 'Testing connection'); out.appendChild(sk);
      btn.disabled = true; btn.classList.add('is-busy');
      var spin = make('span', 'spinner'); spin.setAttribute('aria-hidden', 'true'); btn.insertBefore(spin, btn.firstChild);
      window.strandApi('POST', '/api/v1/pipeline-connections/test', body)
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, status: r.status, j: j }; }, function () { return { ok: false, status: r.status, j: {} }; }); })
        .then(function (res) { showResult(out, res); })
        .catch(function (e) { showResult(out, { ok: false, status: 0, j: { error: String(e) } }); })
        .then(function () { btn.disabled = false; btn.classList.remove('is-busy'); if (spin.parentNode) { spin.remove(); } });
    }
    function tone(status) { return status === 'ok' ? 'st-success' : (status === 'warn' ? 'st-warning' : 'st-danger'); }
    function showResult(out, res) {
      out.textContent = '';
      var j = res.j || {};
      if (!res.ok) {
        var es = make('div', 'error-state'); es.setAttribute('role', 'alert');
        es.appendChild(make('b', '', res.status === 403 ? 'Not allowed' : 'The test could not run'));
        es.appendChild(make('span', 'wrap', j.error || 'Request failed (' + res.status + ').'));
        out.appendChild(es);
        return;
      }
      var head = make('div', 'conn-head');
      head.appendChild(make('span', 'badge ' + (j.ok ? 'st-success' : 'st-danger'), j.ok ? 'Connected' : 'Failed'));
      var meta = [];
      if (j.version) { meta.push(j.version); }
      if (j.latency_ms) { meta.push(j.latency_ms + ' ms'); }
      if (meta.length) { head.appendChild(make('span', 'muted small', ' ' + meta.join(' · '))); }
      out.appendChild(head);
      if (!j.ok && j.error) { out.appendChild(make('p', 'err small wrap', j.error)); }
      if (j.missing_privileges && j.missing_privileges.length) {
        out.appendChild(make('p', 'small', 'Missing privileges: ' + j.missing_privileges.join(', ')));
      }
      if (j.checks && j.checks.length) {
        var ul = make('ul');
        j.checks.forEach(function (c) {
          var li = make('li');
          li.appendChild(make('span', 'badge small ' + tone(c.status), c.status));
          li.appendChild(make('span', '', c.name + (c.detail ? ': ' + c.detail : '')));
          ul.appendChild(li);
        });
        out.appendChild(ul);
      }
    }
    Array.prototype.forEach.call(form.querySelectorAll('[data-test]'), function (b) {
      b.addEventListener('click', function () { testConn(b.getAttribute('data-test')); });
    });

    // ---- API errors: attach to the field, open its step ----
    function showApiError(j) {
      var fields = (j && j.fields) || {}, first = null;
      Object.keys(fields).forEach(function (n) {
        var f = form.querySelector('[name="' + n + '"]:not([disabled])');
        if (f) { showErr(f, fields[n]); if (!first) { first = f; } }
      });
      if (first) { goto(stepOf(first)); first.focus(); }
    }
    function busy(on) {
      if (on) { form.setAttribute('aria-busy', 'true'); } else { form.removeAttribute('aria-busy'); }
      create.disabled = on;
      create.classList.toggle('is-busy', on);
      var sp = create.querySelector('.spinner');
      if (on && !sp) { sp = make('span', 'spinner'); sp.setAttribute('aria-hidden', 'true'); create.insertBefore(sp, create.firstChild); }
      if (!on && sp) { sp.remove(); }
      create.lastChild.textContent = on ? 'Creating...' : 'Create and validate';
    }
    window.strandWizard = { validateAll: validateAll, showApiError: showApiError, busy: busy, goto: goto, step: function () { return cur; } };
    goto(1);
  }

  // ---------------------------------------------------------------- Pipelines list: selection and bulk actions
  function bulk() {
    if (!$('pl-bulkbar')) { return; }
    var selected = {};
    var START = ['stopped', 'failed', 'unknown'];
    function rows() { return Array.prototype.slice.call(document.querySelectorAll('#pipeline-table tbody tr[data-name]')); }
    function sync() {
      var all = $('pl-selall'), n = 0, vis = 0;
      rows().forEach(function (r) {
        var cb = r.querySelector('.rowcheck'), name = r.getAttribute('data-name');
        if (!cb) { return; }
        if (r.hidden) { delete selected[name]; }
        cb.checked = !!selected[name];
        if (!r.hidden) { vis++; }
        if (selected[name]) { n++; }
      });
      Object.keys(selected).forEach(function (k) { if (!rows().some(function (r) { return r.getAttribute('data-name') === k; })) { delete selected[k]; } });
      var bar = $('pl-bulkbar'); // looked up again: a content refresh replaces the element
      if (bar) { bar.hidden = n === 0; }
      var c = $('pl-selcount'); if (c) { c.textContent = String(n); }
      if (all) { all.checked = vis > 0 && n === vis; all.indeterminate = n > 0 && n < vis; }
    }
    document.addEventListener('change', function (ev) {
      var t = ev.target;
      if (!t) { return; }
      if (t.id === 'pl-selall') {
        rows().forEach(function (r) { if (!r.hidden && r.querySelector('.rowcheck')) { if (t.checked) { selected[r.getAttribute('data-name')] = true; } else { delete selected[r.getAttribute('data-name')]; } } });
        sync();
      } else if (t.classList && t.classList.contains('rowcheck')) {
        if (t.checked) { selected[t.getAttribute('data-name')] = true; } else { delete selected[t.getAttribute('data-name')]; }
        sync();
      }
    });
    function stateOf(name) {
      var r = rows().filter(function (x) { return x.getAttribute('data-name') === name; })[0];
      return r ? r.getAttribute('data-state') : '';
    }
    function run(action) {
      var names = Object.keys(selected).sort();
      var ok = names.filter(function (n) { var st = stateOf(n); return action === 'start' ? START.indexOf(st) >= 0 : START.indexOf(st) < 0; });
      var skipped = names.length - ok.length;
      if (!ok.length) { toast('None of the selected pipelines can be ' + (action === 'start' ? 'started' : 'stopped') + ' (they are ' + (action === 'start' ? 'already running' : 'not running') + ').', false); return; }
      var noun = ok.length === 1 ? '1 pipeline' : ok.length + ' pipelines';
      var msg = (action === 'start' ? 'Start ' : 'Stop ') + noun + ': ' + ok.join(', ') + '.' + (skipped ? ' ' + skipped + ' selected skipped (' + (action === 'start' ? 'already running' : 'not running') + ').' : '') +
        (action === 'stop' ? ' Each one flushes in-flight batches and writes its checkpoint first (up to 150 s) and resumes from it on the next start.' : '');
      var phrase = action === 'stop' ? 'stop ' + ok.length : '';
      con().confirm(msg, phrase, { title: (action === 'start' ? 'Start ' : 'Stop ') + noun, ok: action === 'start' ? 'Start' : 'Stop', danger: action === 'stop' }).then(function (go) {
        if (!go) { return; }
        var done = 0, failed = [];
        toast((action === 'start' ? 'Starting ' : 'Stopping ') + noun + '...', true);
        ok.reduce(function (p, name) {
          return p.then(function () {
            return window.strandApi('POST', '/api/v1/pipelines/' + encodeURIComponent(name) + '/actions/' + action, {})
              .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }, function () { return { ok: r.ok, j: {} }; }); })
              .then(function (res) { if (res.ok) { done++; } else { failed.push(name + ': ' + ((res.j && res.j.error) || 'failed')); } })
              .catch(function (e) { failed.push(name + ': ' + e); });
          });
        }, Promise.resolve()).then(function () {
          if (failed.length) { toast((action === 'start' ? 'Started ' : 'Stopped ') + done + ', failed ' + failed.length + ' (' + failed.join('; ') + ')', false); }
          else { toast((action === 'start' ? 'Started ' : 'Stopped ') + noun + '.', true); }
          selected = {};
          sync();
          setTimeout(function () { if (con().refresh) { con().refresh(true); } }, 1500);
        });
      });
    }
    document.addEventListener('click', function (ev) {
      var b = ev.target && ev.target.closest && ev.target.closest('[data-bulk]');
      if (!b) { return; }
      var a = b.getAttribute('data-bulk');
      if (a === 'clear') { selected = {}; sync(); }
      else { run(a); }
    });
    document.addEventListener('strand:list-applied', sync);
    if (con().onSwap) { con().onSwap.push(sync); }
    sync();
  }

  var form = $('new-pipeline-form');
  if (form) { wizard(form); }
  bulk();
})();
