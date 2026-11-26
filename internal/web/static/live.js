// Strand web console: live data. The server's background sampler is the only thing that talks to the
// pipelines; this script subscribes to its Server-Sent Events (GET /api/v1/stream) and updates numbers,
// badges, bars, sparklines, charts, the log tail and the error list IN PLACE (elements marked with data-bind*),
// so focus, scroll position and text selection are never disturbed. When EventSource is missing or keeps
// failing it polls GET /api/v1/live instead. Schema: docs/WEB-API.md "Live stream". No libraries.
(function () {
  'use strict';
  var content = document.getElementById('content');
  if (!content || !content.getAttribute('data-live')) { return; }
  var target = content.getAttribute('data-live');       // "all" or a pipeline name
  var page = target === 'all' ? '' : target;            // pipeline whose full record this page shows
  var interval = parseFloat(content.getAttribute('data-interval') || '5') || 5;
  var wantLogs = content.hasAttribute('data-live-logs');
  var hooks = window.strandConsole || {};
  var SCOPES = '[data-live-row],[data-live-head],[data-live-nav],[data-live-overview]';
  var BINDS = '[data-bind],[data-bind-badge],[data-bind-bar],[data-bind-show],[data-bind-hide],[data-bind-dot],[data-stale-tag],[data-bind-age],[data-spark]';

  var seen = {};          // pipeline -> sampled_at (ms) of the newest record applied
  var okAt = {};          // pipeline -> local ms of the last good sample (for "updated N s ago")
  var lastEvent = 0;      // local ms of the last message from the server
  var structuralPending = false;

  // ---------- small helpers ----------
  function setText(el, s) {
    if (s === undefined || s === null) { return; }
    s = String(s);
    if (el.textContent === s) { return; }
    // Keep the text node (and so any selection anchored elsewhere) when the element holds only text.
    if (el.childNodes.length === 1 && el.firstChild.nodeType === 3) { el.firstChild.nodeValue = s; } else { el.textContent = s; }
  }
  function present(v) { return v !== undefined && v !== null && v !== '' && v !== '-'; }
  function relAgo(sec) {
    if (!(sec >= 0)) { return 'no data'; }
    if (sec < 60) { return 'updated ' + Math.floor(sec) + ' s ago'; }
    if (sec < 3600) { return 'updated ' + Math.floor(sec / 60) + ' min ago'; }
    return 'updated ' + Math.floor(sec / 3600) + ' h ago';
  }
  function swapClass(el, prefix, cls) {
    Array.prototype.slice.call(el.classList).forEach(function (c) { if (c.indexOf(prefix) === 0) { el.classList.remove(c); } });
    if (cls) { el.classList.add(cls); }
  }
  // scope of a bound element: the pipeline (or "overview") whose data it shows
  function scopeOf(el) {
    var s = el.closest(SCOPES);
    if (!s) { return page ? 'p:' + page : ''; }
    if (s.hasAttribute('data-live-overview')) { return 'overview'; }
    return 'p:' + (s.getAttribute('data-live-row') || s.getAttribute('data-live-head') || s.getAttribute('data-live-nav'));
  }
  function setBar(holder, pct) {
    var svg = holder.tagName.toLowerCase() === 'svg' ? holder : holder.querySelector('svg.pbar');
    if (!svg || isNaN(pct)) { return; }
    pct = Math.max(0, Math.min(100, pct));
    var fg = svg.querySelector('rect.fg');
    var w = pct.toFixed(2);
    if (fg && fg.getAttribute('width') !== w) { fg.setAttribute('width', w); }
    svg.classList.toggle('done', pct >= 100);
    var lbl = svg.getAttribute('aria-label') || '';
    var m = lbl.match(/^(.*?:\s*)?[\d.]+%$/);
    var nl = (m && m[1] ? m[1] : '') + pct.toFixed(1) + '%';
    if (lbl !== nl) { svg.setAttribute('aria-label', nl); }
  }
  function barSVG(pct) {
    var ns = 'http://www.w3.org/2000/svg';
    var svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('class', 'pbar'); svg.setAttribute('viewBox', '0 0 100 8'); svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', '0.0%');
    var bg = document.createElementNS(ns, 'rect'); bg.setAttribute('class', 'bg'); bg.setAttribute('x', '0'); bg.setAttribute('y', '0'); bg.setAttribute('width', '100'); bg.setAttribute('height', '8');
    var fg = document.createElementNS(ns, 'rect'); fg.setAttribute('class', 'fg'); fg.setAttribute('x', '0'); fg.setAttribute('y', '0'); fg.setAttribute('width', '0'); fg.setAttribute('height', '8');
    svg.appendChild(bg); svg.appendChild(fg);
    setBar(svg, pct);
    return svg;
  }

  // ---------- sparkline (overview / pipelines table) ----------
  function drawSpark(svg, vals) {
    var vb = (svg.getAttribute('viewBox') || '0 0 90 22').split(/\s+/);
    var w = parseFloat(vb[2]) || 90, h = parseFloat(vb[3]) || 22, ns = 'http://www.w3.org/2000/svg';
    if (vals.length < 2) { return; }
    var max = 0;
    vals.forEach(function (v) { if (v > max) { max = v; } });
    if (max <= 0) { max = 1; }
    var pts = vals.map(function (v, i) {
      return ((w - 2) * i / (vals.length - 1) + 1).toFixed(1) + ',' + ((h - 2) * (1 - v / max) + 1).toFixed(1);
    });
    var pl = svg.querySelector('polyline'), c = svg.querySelector('circle');
    if (!pl) { pl = document.createElementNS(ns, 'polyline'); pl.setAttribute('class', 'line s1'); svg.appendChild(pl); }
    if (!c) { c = document.createElementNS(ns, 'circle'); c.setAttribute('class', 'pt s1'); c.setAttribute('r', '1.8'); svg.appendChild(c); }
    pl.setAttribute('points', pts.join(' '));
    var last = pts[pts.length - 1].split(',');
    c.setAttribute('cx', last[0]); c.setAttribute('cy', last[1]);
  }
  function pushSpark(svg, v) {
    var vals = (svg.getAttribute('data-spark') || '').split(',').filter(function (x) { return x !== ''; }).map(parseFloat);
    vals.push(isFinite(v) ? v : 0);
    if (vals.length > 60) { vals = vals.slice(vals.length - 60); }
    svg.setAttribute('data-spark', vals.map(function (x) { return +x.toPrecision(6); }).join(','));
    drawSpark(svg, vals);
  }

  // ---------- applying one pipeline record ----------
  function bindEl(el, f, rec) {
    var k;
    if ((k = el.getAttribute('data-bind')) !== null && f[k] !== undefined) {
      setText(el, f[k]);
      if (el.hasAttribute('data-bind-lag')) {
        el.classList.toggle('lag-warn', rec.lag_level === 'warn');
        el.classList.toggle('lag-bad', rec.lag_level === 'bad');
      }
      if (k === 'reason') { el.hidden = !present(f[k]) && el.classList.contains('reason'); if (el.title !== f[k]) { el.title = f[k]; } }
    }
    if ((k = el.getAttribute('data-bind-badge')) !== null && f[k] !== undefined) {
      swapClass(el, 'st-', 'st-' + f[k]);
      setText(el, f[k]);
    }
    if ((k = el.getAttribute('data-bind-dot')) !== null && f[k] !== undefined) {
      swapClass(el, 'st-', 'st-' + f[k]);
      if (el.getAttribute('aria-label') !== f[k]) { el.setAttribute('aria-label', f[k]); }
    }
    if ((k = el.getAttribute('data-bind-bar')) !== null && f[k] !== undefined && f[k] !== '') { setBar(el, parseFloat(f[k])); }
    if ((k = el.getAttribute('data-bind-show')) !== null) { el.hidden = !present(f[k]); }
    if ((k = el.getAttribute('data-bind-hide')) !== null) { el.hidden = present(f[k]); }
    if (el.hasAttribute('data-stale-tag')) { el.hidden = !rec.stale; }
    if (el.hasAttribute('data-bind-age') && rec.name) {
      el.setAttribute('data-ok', okAt[rec.name] ? String(okAt[rec.name]) : '');
      tickAge(el);
    }
    if (el.hasAttribute('data-spark') && rec.pt) { pushSpark(el, rec.pt.lag_seconds); }
  }
  function bindScope(root, scope, f, rec) {
    var els = Array.prototype.slice.call(root.querySelectorAll(BINDS));
    if (root.matches && root.matches(BINDS)) { els.unshift(root); }
    els.forEach(function (el) { if (scopeOf(el) === scope) { bindEl(el, f, rec); } });
  }

  function applyRecord(rec, receivedAt) {
    if (!rec || !rec.name) { return; }
    var t = Date.parse(rec.sampled_at) || 0;
    if (seen[rec.name] && t < seen[rec.name]) { return; } // an older record (replay after a resync): ignore
    seen[rec.name] = t;
    if (rec.last_ok) { okAt[rec.name] = receivedAt - (rec.age_s || 0) * 1000; } else { delete okAt[rec.name]; }
    var scope = 'p:' + rec.name, f = rec.f || {};
    var sel = '[data-live-row="' + cssEsc(rec.name) + '"],[data-live-head="' + cssEsc(rec.name) + '"],[data-live-nav="' + cssEsc(rec.name) + '"]';
    Array.prototype.forEach.call(document.querySelectorAll(sel), function (root) {
      root.classList.toggle('is-stale', !!rec.stale);
      if (root.hasAttribute('data-live-row') && root.tagName === 'TR') {
        root.setAttribute('data-state', rec.state);
        root.setAttribute('data-sev', String(rec.sev));
        root.setAttribute('data-lag', rec.lag_s === undefined ? '-1' : String(rec.lag_s));
        root.setAttribute('data-rate', rec.lag_s === undefined ? '-1' : String(rec.rows_s));
      }
      if (root.hasAttribute('data-live-nav')) { root.title = rec.name + ': ' + rec.state; }
      bindScope(root, scope, f, rec);
      if (root.hasAttribute('data-live-head')) {
        var evidence = root.querySelector('[data-verification-status]');
        if ((evidence && !f.verification_status) || (f.verification_status && (!evidence ||
            evidence.getAttribute('data-verification-status') !== f.verification_status ||
            evidence.getAttribute('data-verification-at') !== f.verification_at))) { structural(); }
        var was = root.getAttribute('data-state');
        if (was && was !== rec.state) { root.setAttribute('data-state', rec.state); structural(); } // buttons depend on the state
      }
    });
    if (rec.name === page) {
      bindScope(content, scope, f, rec);
      if (rec.table_rows || rec.tables_changed) { applyTables(rec.table_rows, rec.tables_full, rec.tables_changed); }
      if (rec.events_new) { applyEvents(rec.events_new); }
      if (rec.pt) { chartPush(rec.pt); }
    }
  }
  function cssEsc(s) { return String(s).replace(/["\\]/g, '\\$&'); }

  function applyOverview(ov) {
    if (!ov) { return; }
    var f = ov.f || {};
    f.tables = ov.tables_total > 0 ? f.tables_pct : '';
    Array.prototype.forEach.call(document.querySelectorAll('[data-live-overview]'), function (root) {
      bindScope(root, 'overview', f, {});
      Array.prototype.forEach.call(root.querySelectorAll('[data-state-chip]'), function (chip) {
        var st = chip.getAttribute('data-state-chip'), n = (ov.counts || {})[st] || 0;
        chip.hidden = !(n > 0 || st === 'synced' || st === 'failed' || st === 'stopped');
      });
      var k = root.querySelector('[data-attn-kpi]');
      if (k) { k.classList.toggle('bad', ov.attention.length > 0); k.classList.toggle('ok', ov.attention.length === 0); }
    });
    // the attention list and the pipeline list are structure: re-render the page when they change
    var attn = document.querySelector('[data-attn]');
    if (attn && attn.getAttribute('data-attn') !== (ov.attention || []).join(',')) { structural(); }
    var rows = document.querySelectorAll('tr[data-live-row]');
    if (document.getElementById('pipeline-table') && rows.length !== (ov.pipelines || []).length) { structural(); }
  }

  // ---------- Tables tab ----------
  function applyTables(rows, full, changed) {
    var table = document.getElementById('tables-table');
    if (!table) { return; }
    if (table.hasAttribute('data-paged')) { applyTablesPaged(table, rows || [], changed); return; }
    if (!rows) { if (changed) { structural(); } return; }
    var known = {};
    Array.prototype.forEach.call(table.querySelectorAll('tr[data-table]'), function (tr) { known[tr.getAttribute('data-table')] = tr; });
    var missing = false;
    rows.forEach(function (r) {
      var tr = known[r.table];
      if (!tr) { missing = true; return; }
      tr.setAttribute('data-state', r.state);
      tr.setAttribute('data-pct', r.pct.toFixed(2));
      tr.setAttribute('data-rows', String(r.rows_estimate));
      tr.setAttribute('data-eta', r.eta_s > 0 ? r.eta_s.toFixed(0) : '-1');
      tr.classList.toggle('is-running', r.state === 'running');
      setActivity(tr, r);
      Array.prototype.forEach.call(tr.querySelectorAll('[data-tb]'), function (el) {
        var k = el.getAttribute('data-tb');
        if (k === 'state') { swapClass(el, 'st-tbl-', 'st-tbl-' + r.state); setText(el, r.state); }
        else if (k === 'bar') { setBar(el, r.pct); }
        else if (k === 'last') { setLastCell(el, r); }
        else if (r.f[k] !== undefined) { setText(el, r.f[k]); }
      });
    });
    if (full && (missing || rows.length !== Object.keys(known).length)) { structural(); return; }
    if (missing) { structural(); return; }
    renderRunning(table);
    if (hooks.applyTables && !document.activeElement.closest('#tables-table')) { hooks.applyTables(); }
  }
  // Paged list: update the rows of this page in place, keep "Copying now" from the running rows the server sends;
  // re-render the page (same filter/sort/page, server side) when the list changed or a visible row changed state.
  function applyTablesPaged(table, rows, changed) {
    var known = {}, restate = false;
    Array.prototype.forEach.call(table.querySelectorAll('tr[data-table]'), function (tr) { known[tr.getAttribute('data-table')] = tr; });
    rows.forEach(function (r) {
      var tr = known[r.table];
      if (tr) {
        if (tr.getAttribute('data-state') !== r.state) { restate = true; }
        tr.setAttribute('data-pct', r.pct.toFixed(2));
        setActivity(tr, r);
        Array.prototype.forEach.call(tr.querySelectorAll('[data-tb]'), function (el) {
          var k = el.getAttribute('data-tb');
          if (k === 'bar') { setBar(el, r.pct); } else if (k === 'last') { setLastCell(el, r); }
          else if (k !== 'state' && r.f[k] !== undefined) { setText(el, r.f[k]); }
        });
      }
      runlistPut(r);
    });
    if (changed || restate) { structural(); }
  }
  // CDC activity of a table row: data-last / data-changes (filters and sorts), the Changes tooltip.
  function setActivity(tr, r) {
    if (r.last_ms === undefined) { return; }
    tr.setAttribute('data-last', String(r.last_ms));
    tr.setAttribute('data-changes', String(r.changes || 0));
    // Quiet badge (server's quiet-table rule): rendered only for quiet rows, added/removed here.
    if (r.quiet) { tr.setAttribute('data-quiet', '1'); } else { tr.removeAttribute('data-quiet'); }
    var qb = tr.querySelector('[data-tb="quiet"]'), lc = tr.querySelector('[data-tb="last"]');
    if (r.quiet && !qb && lc) {
      qb = document.createElement('span'); qb.className = 'badge st-warn small quiet-badge'; qb.setAttribute('data-tb', 'quiet');
      qb.title = 'Unusually idle (see quiet in the summary above)'; qb.textContent = 'quiet';
      lc.parentNode.appendChild(document.createTextNode(' ')); lc.parentNode.appendChild(qb);
    } else if (!r.quiet && qb) {
      var sp = qb.previousSibling;
      if (sp && sp.nodeType === 3 && !sp.nodeValue.trim()) { sp.parentNode.removeChild(sp); }
      qb.parentNode.removeChild(qb);
    }
    var chg = tr.querySelector('[data-tb="chg"]');
    if (chg && r.f.chg_title) { (chg.closest('td') || chg).title = r.f.chg_title; }
  }
  // Last sync cell: a <time data-ago> the relative-time ticker keeps current, or "-" without a change.
  function setLastCell(el, r) {
    if (r.last_ms === undefined) { return; }
    var t = el.querySelector('time');
    if (r.last_ms < 0) {
      if (t || !el.textContent) { el.textContent = ''; var m = document.createElement('span'); m.className = 'muted'; m.textContent = '-'; el.appendChild(m); }
      if (el.firstChild && r.f.last_title) { el.firstChild.title = r.f.last_title; }
      return;
    }
    if (!t) { el.textContent = ''; t = document.createElement('time'); el.appendChild(t); }
    if (t.getAttribute('data-ago') !== String(r.last_ms)) {
      t.setAttribute('data-ago', String(r.last_ms));
      t.dateTime = new Date(r.last_ms).toISOString();
    }
    if (r.f.last_title) { t.title = r.f.last_title; }
    setText(t, relAgo(r.last_ms));
  }
  // relAgo: like the server's fmtRel ("12 s ago", "5 min ago", "3 h ago", "2 d ago"), on the server clock.
  function relAgo(ms) {
    var d = (Date.now() + (window.strandSkew || 0) - ms) / 1000;
    if (d < 0) { return 'just now'; }
    if (d < 60) { return Math.floor(d) + ' s ago'; }
    if (d < 3600) { return Math.floor(d / 60) + ' min ago'; }
    if (d < 48 * 3600) { return Math.floor(d / 3600) + ' h ago'; }
    return Math.floor(d / 86400) + ' d ago';
  }
  window.strandRelAgo = relAgo;
  setInterval(function () {
    if (document.hidden) { return; }
    Array.prototype.forEach.call(document.querySelectorAll('time[data-ago]'), function (t) {
      var ms = parseFloat(t.getAttribute('data-ago'));
      if (!isNaN(ms)) { setText(t, relAgo(ms)); }
    });
  }, 5000);
  function runlistPut(r) {
    var list = document.getElementById('runlist');
    if (!list) { return; }
    var li = null;
    Array.prototype.forEach.call(list.querySelectorAll('li[data-run]'), function (x) { if (x.getAttribute('data-run') === r.table) { li = x; } });
    if (r.state !== 'running') { if (li) { list.removeChild(li); } }
    else {
      if (!li) {
        if (list.children.length >= 50) { return; }
        li = document.createElement('li'); li.setAttribute('data-run', r.table);
        var n = document.createElement('span'); n.className = 'mono wrap rl-name'; n.textContent = r.table;
        var b = document.createElement('span'); b.className = 'rl-bar'; b.appendChild(barSVG(0));
        li.appendChild(n); li.appendChild(b);
        ['c-num rl-pct', 'c-num small muted rl-rows', 'c-num small rl-eta'].forEach(function (c) { var e = document.createElement('span'); e.className = c; li.appendChild(e); });
        list.appendChild(li);
      }
      setBar(li.querySelector('.rl-bar'), r.pct);
      setText(li.querySelector('.rl-pct'), r.f.pct);
      setText(li.querySelector('.rl-rows'), r.f.rows);
      setText(li.querySelector('.rl-eta'), r.f.eta);
    }
    var empty = document.getElementById('runlist-empty');
    if (empty) { empty.hidden = list.querySelectorAll('li[data-run]').length > 0; }
  }
  // "Copying now": one line per running table (several run in parallel), updated in place, keyed by name.
  function renderRunning(table) {
    var list = document.getElementById('runlist');
    if (!list) { return; }
    var running = Array.prototype.filter.call(table.querySelectorAll('tr[data-table]'), function (tr) { return tr.getAttribute('data-state') === 'running'; });
    var have = {};
    Array.prototype.forEach.call(list.querySelectorAll('li[data-run]'), function (li) { have[li.getAttribute('data-run')] = li; });
    var keep = {};
    running.forEach(function (tr) {
      var name = tr.getAttribute('data-table'), li = have[name];
      keep[name] = true;
      if (!li) {
        li = document.createElement('li'); li.setAttribute('data-run', name);
        var n = document.createElement('span'); n.className = 'mono wrap rl-name'; n.textContent = name;
        var b = document.createElement('span'); b.className = 'rl-bar'; b.appendChild(barSVG(0));
        var p = document.createElement('span'); p.className = 'c-num rl-pct';
        var r = document.createElement('span'); r.className = 'c-num small muted rl-rows';
        var e = document.createElement('span'); e.className = 'c-num small rl-eta';
        li.appendChild(n); li.appendChild(b); li.appendChild(p); li.appendChild(r); li.appendChild(e);
        list.appendChild(li);
      }
      var pct = parseFloat(tr.getAttribute('data-pct')) || 0;
      setBar(li.querySelector('.rl-bar'), pct);
      var cell = function (k) { var c = tr.querySelector('[data-tb="' + k + '"]'); return c ? c.textContent : ''; };
      setText(li.querySelector('.rl-pct'), cell('pct'));
      setText(li.querySelector('.rl-rows'), cell('rows'));
      setText(li.querySelector('.rl-eta'), cell('eta'));
    });
    Object.keys(have).forEach(function (name) { if (!keep[name]) { list.removeChild(have[name]); } });
    var empty = document.getElementById('runlist-empty');
    if (empty) { empty.hidden = running.length > 0; }
  }

  // ---------- Errors tab ----------
  function applyEvents(evs) {
    var body = document.getElementById('live-events');
    if (!body || !evs.length) { return; }
    var wrap = document.getElementById('live-events-wrap');
    var atEnd = !wrap || wrap.scrollTop + wrap.clientHeight >= wrap.scrollHeight - 8;
    var have = {};
    Array.prototype.forEach.call(body.querySelectorAll('tr[data-ev]'), function (tr) { have[tr.getAttribute('data-ev')] = true; });
    var empty = body.querySelector('tr.empty-row');
    evs.forEach(function (e) {
      var key = e.time + '|' + e.level + '|' + e.message;
      if (have[key]) { return; }
      have[key] = true;
      var tr = document.createElement('tr'); tr.setAttribute('data-ev', key); tr.className = 'is-new';
      var t = document.createElement('td'); t.className = 'small nowrap';
      var localTime = window.strandLocalISO(e.time);
      t.textContent = localTime ? localTime.replace('T', ' ') + ' UTC+7' : e.time;
      var l = document.createElement('td'); var b = document.createElement('span'); b.className = 'badge lv-' + String(e.level).toLowerCase(); b.textContent = e.level; l.appendChild(b);
      var m = document.createElement('td'); m.className = 'wrap msg'; m.textContent = e.message;
      tr.appendChild(t); tr.appendChild(l); tr.appendChild(m);
      body.insertBefore(tr, empty);
    });
    var rows = body.querySelectorAll('tr[data-ev]');
    for (var i = 0; i < rows.length - 200; i++) { body.removeChild(rows[i]); }
    if (empty) { empty.hidden = rows.length > 0; }
    if (wrap && atEnd) { wrap.scrollTop = wrap.scrollHeight; }
  }

  // ---------- Logs tab: live tail, follow toggle, pause on scroll-up ----------
  var logPre = null, logFollow = null, logStatus = null, logJump = null, logPending = 0, logLines = [], logError = "";
  function logAttach() {
    logPre = document.getElementById('logtail');
    logFollow = document.getElementById('log-follow');
    logStatus = document.getElementById('log-status');
    logJump = document.getElementById('log-jump');
    logPending = 0;
    logError = "";
    if (!logPre) { return; }
    logLines = logPre.textContent.replace(/\n$/, '').split('\n');
    if (logLines.length === 1 && logLines[0] === '(no log lines)') { logLines = []; }
    if (hooks.paintLog) { hooks.paintLog(logPre); }
    if (logFollow && logFollow.checked) { logPre.scrollTop = logPre.scrollHeight; }
    logPre.addEventListener('scroll', function () {
      if (logAtEnd()) { logPending = 0; logStat(); }
      else { logStat(); }
    });
    if (logFollow) { logFollow.addEventListener('change', function () { if (logFollow.checked) { toEnd(); } logStat(); }); }
    if (logJump) { logJump.addEventListener('click', function () { if (logFollow) { logFollow.checked = true; } toEnd(); logStat(); }); }
    logStat();
  }
  function logAtEnd() { return logPre.scrollTop + logPre.clientHeight >= logPre.scrollHeight - 4; }
  function toEnd() { logPre.scrollTop = logPre.scrollHeight; logPending = 0; }
  function logStat() {
    if (!logStatus) { return; }
    var following = logFollow ? logFollow.checked : true;
    var paused = following && !logAtEnd() && logPre.scrollHeight > logPre.clientHeight;
    var txt = !following ? 'Not following' : (paused ? 'Paused while you scroll' : 'Following the live tail');
    if (logPending > 0) { txt += ' \u00b7 ' + logPending + ' new line' + (logPending === 1 ? '' : 's') + ' below'; }
    if (logError) { txt = 'Log unavailable: ' + logError; }
    else if (conn === 'polling') { txt = 'Metrics are polling. Reload this page to update the log.'; }
    else if (conn !== 'live') { txt = 'Live log disconnected. Showing the last loaded lines.'; }
    setText(logStatus, txt);
    if (logJump) { logJump.hidden = !(logPending > 0 || paused); }
  }
  // suffix of `have` that equals the prefix of `lines` (lines already shown)
  function overlap(have, lines) {
    for (var k = Math.min(have.length, lines.length); k > 0; k--) {
      var ok = true;
      for (var i = 0; i < k; i++) { if (have[have.length - k + i] !== lines[i]) { ok = false; break; } }
      if (ok) { return k; }
    }
    return 0;
  }
  function applyLog(ev) {
    if (!logPre) { return; }
    logError = ev.error || '';
    if (logError) { logStat(); return; }
    var lines = ev.lines || [];
    if (ev.initial) { lines = lines.slice(overlap(logLines, lines)); }
    if (!lines.length) { logStat(); return; }
    if (logFollow && !logFollow.checked) { logPending += lines.length; logStat(); return; } // not following: leave the text alone
    var stick = logAtEnd();
    if (ev.gap) { lines.unshift('… (lines skipped)'); }
    logLines = logLines.concat(lines).slice(-2000);
    var text = logLines.join('\n') + '\n';
    if (hooks.paintLog) { hooks.paintLog(logPre, text); }
    else { logPre.textContent = text; }
    if (stick) { toEnd(); } else { logPending += lines.length; }
    logStat();
  }

  // ---------- Graph tab: charts redrawn from the series, new points appended live ----------
  var CHARTS = {
    lag: { title: 'Replication lag', unit: 'seconds', series: [{ name: 'lag', cls: 's1', key: 'lag_seconds' }] },
    rate: { title: 'Throughput', unit: 'rows/s', series: [{ name: 'applied, 30 s avg', cls: 's1', key: 'rows_per_s' }, { name: 'snapshot, 30 s avg', cls: 's2', key: 'snapshot_rows_per_s' }] },
    holes: { title: 'Holes and writeset', unit: 'count', series: [{ name: 'holes', cls: 's3', key: 'holes' }, { name: 'writeset entries', cls: 's2', key: 'writeset_entries' }] },
    spool: { title: 'Spool', unit: 'bytes', series: [{ name: 'spool bytes', cls: 's4', key: 'spool_bytes' }] }
  };
  // Ranges above 1 h (6 h, 24 h, 7 d; only offered with the PostgreSQL store) come from the history API:
  // bucket averages (lag = bucket maximum), live points appended after the last bucket.
  var HIST = { 21600: '6h', 86400: '24h', 604800: '7d' };
  var series = null, rangeS = 900, chartTimer = null, hist = null, histKey = null, histAt = 0, histBusy = false;
  try { rangeS = parseInt(localStorage.getItem('strand-chart-range') || '900', 10) || 900; } catch (e) { rangeS = 900; }
  function fmtVal(v) {
    var a = Math.abs(v);
    if (a >= 1e9) { return (v / 1e9).toFixed(1) + 'G'; }
    if (a >= 1e6) { return (v / 1e6).toFixed(1) + 'M'; }
    if (a >= 1e4) { return (v / 1e3).toFixed(1) + 'k'; }
    if (a >= 100) { return v.toFixed(0); }
    if (a >= 10) { return v.toFixed(1); }
    if (a === 0) { return '0'; }
    return v.toFixed(2);
  }
  function niceMax(v) {
    if (!(v > 0) || !isFinite(v)) { return 1; }
    var e = Math.floor(Math.log10(v)), f = v / Math.pow(10, e);
    return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10) * Math.pow(10, e);
  }
  function hhmmss(t, long) {
    var local = window.strandLocalISO(t);
    return long ? local.slice(5, 16).replace('T', ' ') : local.slice(11);
  }
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
  // Same geometry as LineChart in chart.go, so the live chart replaces the server one without a jump.
  function chartMarkup(spec, pts) {
    var W = 560, H = 190, L = 52, R = 12, T = 10, B = 24, n = 0, tmin = 0, tmax = 0, vmax = 0;
    spec.series.forEach(function (s) {
      pts.forEach(function (p) { var v = p[s.key]; if (!isFinite(v)) { return; } if (!n || p.t < tmin) { tmin = p.t; } if (!n || p.t > tmax) { tmax = p.t; } if (v > vmax) { vmax = v; } n++; });
    });
    var last = pts.length ? pts[pts.length - 1][spec.series[0].key] : NaN;
    var h = '<figcaption><span class="ctitle">' + esc(spec.title) + '</span> <span class="unit">' + esc(spec.unit) + '</span>' +
      (isFinite(last) ? ' <span class="now">now <b>' + fmtVal(last) + '</b></span>' : '') + '</figcaption>';
    h += '<svg class="linechart" viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="' + esc(spec.title) + '">';
    if (!n) { return h + '<text class="nodata" x="' + W / 2 + '" y="' + H / 2 + '" text-anchor="middle">no data</text></svg>'; }
    var ymax = niceMax(vmax), pw = W - L - R, ph = H - T - B, span = (tmax - tmin) / 1000;
    var X = function (t) { return span <= 0 ? L + pw : L + pw * ((t - tmin) / 1000) / span; };
    var Y = function (v) { return T + ph * (1 - v / ymax); };
    [0, 0.25, 0.5, 0.75, 1].forEach(function (f) {
      var y = Y(ymax * f).toFixed(1);
      h += '<line class="grid" x1="' + L + '" x2="' + (W - R) + '" y1="' + y + '" y2="' + y + '"/>';
      h += '<text class="axis" x="' + (L - 4) + '" y="' + (parseFloat(y) + 3).toFixed(1) + '" text-anchor="end">' + fmtVal(ymax * f) + '</text>';
    });
    [0, 0.5, 1].forEach(function (f, i) {
      var x = (L + pw * f).toFixed(1);
      h += '<line class="tick" x1="' + x + '" x2="' + x + '" y1="' + (H - B) + '" y2="' + (H - B + 3) + '"/>';
      h += '<text class="axis" x="' + x + '" y="' + (H - 6) + '" text-anchor="' + ['start', 'middle', 'end'][i] + '">' + hhmmss(tmin + (tmax - tmin) * f, span > 86400) + '</text>';
    });
    spec.series.forEach(function (s) {
      var xy = [];
      pts.forEach(function (p) { var v = p[s.key]; if (isFinite(v)) { xy.push(X(p.t).toFixed(1) + ',' + Y(v).toFixed(1)); } });
      if (!xy.length) { return; }
      var a = xy[0].split(','), z = xy[xy.length - 1].split(',');
      if (xy.length > 1) {
        h += '<path class="area ' + s.cls + '" d="M' + a[0] + ',' + (H - B) + ' L' + xy.join(' L') + ' L' + z[0] + ',' + (H - B) + ' Z"/>';
        h += '<polyline class="line ' + s.cls + '" points="' + xy.join(' ') + '"><title>' + esc(s.name) + '</title></polyline>';
      }
      h += '<circle class="pt ' + s.cls + '" cx="' + z[0] + '" cy="' + z[1] + '" r="2.5"/>';
    });
    h += '</svg><div class="legend">';
    spec.series.forEach(function (s) { h += '<span class="key ' + s.cls + '">' + esc(s.name) + '</span>'; });
    return h + '</div>';
  }
  function chartsDraw() {
    chartTimer = null;
    var box = document.getElementById('charts');
    if (!box || !series) { return; }
    var key = HIST[rangeS] && document.querySelector('#chart-range [data-range="' + rangeS + '"]') ? HIST[rangeS] : null;
    if (!key && HIST[rangeS]) { rangeS = 900; }
    var cut = Date.now() - rangeS * 1000, pts;
    if (key) {
      if (histKey !== key || Date.now() - histAt > 60000) { histLoad(key); }
      if (histKey !== key || !hist) { return; }
      var lastH = hist.length ? hist[hist.length - 1].t : 0;
      pts = hist.filter(function (p) { return p.t >= cut; }).concat(series.filter(function (p) { return p.t > lastH; }));
    } else {
      pts = series.filter(function (p) { return p.t >= cut; });
    }
    Array.prototype.forEach.call(box.querySelectorAll('[data-chart]'), function (card) {
      var spec = CHARTS[card.getAttribute('data-chart')];
      if (!spec) { return; }
      var fig = card.querySelector('figure.chart');
      if (!fig) { fig = document.createElement('figure'); fig.className = 'chart'; card.appendChild(fig); }
      fig.innerHTML = chartMarkup(spec, pts);
    });
    Array.prototype.forEach.call(document.querySelectorAll('#chart-range [data-range]'), function (b) {
      b.setAttribute('aria-pressed', parseInt(b.getAttribute('data-range'), 10) === rangeS ? 'true' : 'false');
    });
  }
  function chartsSoon() { if (!chartTimer) { chartTimer = setTimeout(chartsDraw, 50); } }
  function histLoad(key) {
    var box = document.getElementById('charts');
    if (!box || histBusy) { return; }
    histBusy = true;
    fetch('/api/v1/pipelines/' + encodeURIComponent(box.getAttribute('data-pipeline')) + '/series?range=' + key, { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        histBusy = false;
        if (j && j.samples && j.range === key) { hist = j.samples.map(toPoint); histKey = key; histAt = Date.now(); chartsDraw(); }
      })
      .catch(function () { histBusy = false; });
  }
  function toPoint(s) {
    var p = { t: Date.parse(s.t) };
    Object.keys(s).forEach(function (k) { if (k !== 't' && typeof s[k] === 'number') { p[k] = s[k]; } });
    return p;
  }
  function chartsLoad() {
    var box = document.getElementById('charts');
    if (!box) { return; }
    fetch('/api/v1/pipelines/' + encodeURIComponent(box.getAttribute('data-pipeline')) + '/series', { credentials: 'same-origin' })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { if (j && j.samples) { series = j.samples.map(toPoint); chartsDraw(); } })
      .catch(function () { /* keep the server-rendered charts */ });
  }
  function chartPush(pt) {
    if (!series) { return; }
    var p = toPoint(pt);
    if (series.length && p.t <= series[series.length - 1].t) { return; }
    series.push(p);
    var cut = p.t - 3600 * 1000;
    while (series.length && series[0].t < cut) { series.shift(); }
    chartsSoon();
  }
  document.addEventListener('click', function (ev) {
    var b = ev.target && ev.target.closest && ev.target.closest('#chart-range [data-range]');
    if (!b) { return; }
    rangeS = parseInt(b.getAttribute('data-range'), 10) || 900;
    try { localStorage.setItem('strand-chart-range', String(rangeS)); } catch (e) { /* storage blocked */ }
    chartsDraw();
  });

  // ---------- source tables, loaded only when the operator asks (the page render never queries the source) ----------
  function loadSrc(holder, done) {
    var url = holder.getAttribute('data-src-tables');
    if (!url) { return; }
    holder.removeAttribute('data-src-tables');
    fetch(url, { credentials: 'same-origin' }).then(function (r) { return r.json(); }).then(function (j) { done(j); })
      .catch(function (e) { done({ error: String(e), tables: [] }); });
  }
  document.addEventListener('click', function (ev) {
    var b = ev.target && ev.target.closest && ev.target.closest('[data-load-src]');
    if (!b) { return; }
    var holder = b.closest('[data-src-tables]');
    if (!holder) { return; }
    b.disabled = true;
    setText(b, 'Loading\u2026');
    loadSrc(holder, function (j) {
      holder.textContent = '';
      if (j.error) { var p = document.createElement('span'); p.className = 'muted'; p.textContent = j.error; holder.appendChild(p); return; }
      var any = false;
      (j.tables || []).forEach(function (t) {
        if (t.in_pipeline) { return; }
        any = true;
        var l = document.createElement('label'); l.className = 'pick';
        var cb = document.createElement('input'); cb.type = 'checkbox'; cb.name = 'tables'; cb.value = t.table; cb.setAttribute('aria-label', t.table);
        var s = document.createElement('span'); s.className = 'mono'; s.textContent = t.table;
        l.appendChild(cb); l.appendChild(document.createTextNode(' ')); l.appendChild(s);
        holder.appendChild(l);
      });
      if (!any) { var m = document.createElement('span'); m.className = 'muted'; m.textContent = 'No source tables outside the pipeline.'; holder.appendChild(m); }
    });
  });
  document.addEventListener('focusin', function (ev) {
    var inp = ev.target;
    if (!inp || !inp.getAttribute || !inp.getAttribute('list')) { return; }
    var dl = document.getElementById(inp.getAttribute('list'));
    if (!dl || !dl.hasAttribute('data-src-tables')) { return; }
    loadSrc(dl, function (j) {
      (j.tables || []).forEach(function (t) { var o = document.createElement('option'); o.value = t.table; dl.appendChild(o); });
    });
  });

  // ---------- "updated N s ago" and the connection pill ----------
  function tickAge(el) {
    var ok = parseFloat(el.getAttribute('data-ok') || '');
    if (!ok) {
      var iso = el.getAttribute('data-bind-age');
      if (iso) { ok = Date.parse(iso); }
    }
    setText(el, ok ? relAgo((Date.now() - ok) / 1000) : 'no data');
  }
  var pill = document.getElementById('livepill'), pillText = document.getElementById('livetext');
  var conn = 'connecting';
  function setConn(c) {
    conn = c;
    paintPill();
    logStat();
  }
  function paintPill() {
    if (!pill) { return; }
    var quiet = lastEvent && Date.now() - lastEvent > (interval * 3 + 20) * 1000;
    var c = conn === 'live' && quiet ? 'quiet' : conn;
    if (pill.getAttribute('data-conn') !== c) { pill.setAttribute('data-conn', c); }
    var ago = lastEvent ? Math.max(0, Math.floor((Date.now() - lastEvent) / 1000)) : -1;
    var txt = { connecting: 'Connecting', live: 'Live', quiet: 'No new data', reconnecting: 'Reconnecting', polling: 'Live (polling)', offline: 'Offline' }[c] || c;
    setText(pillText, txt);
    pill.title = c === 'polling' ? 'The event stream is unavailable. Checking overview and pipeline telemetry every ' + interval + ' s.' :
      (c === 'live' ? 'Live: the server pushes a sample every ' + interval + ' s' : 'Live data: ' + txt + (ago >= 0 ? ' (last message ' + ago + ' s ago)' : ''));
  }
  setInterval(function () {
    Array.prototype.forEach.call(document.querySelectorAll('[data-stale-tag]:not([hidden]) [data-bind-age]'), tickAge);
    paintPill();
  }, 1000);

  // ---------- structural refresh (pipeline list, attention set, state of this pipeline, new tables) ----------
  function structural() {
    if (structuralPending || !hooks.refresh) { return; }
    structuralPending = true;
    var tryIt = function () {
      if ((hooks.editing && hooks.editing()) || (hooks.selecting && hooks.selecting()) || document.hidden) { setTimeout(tryIt, 2000); return; }
      structuralPending = false;
      hooks.refresh(true);
    };
    setTimeout(tryIt, 300);
  }
  if (hooks.onSwap) {
    hooks.onSwap.push(function () { logAttach(); if (series) { chartsDraw(); } });
  }

  // ---------- transport: SSE, falling back to polling ----------
  function handle(type, data) {
    var now = Date.now();
    var j;
    try { j = JSON.parse(data); } catch (e) { return; }
    lastEvent = now;
    if (type === 'snapshot' || type === 'sample' || type === 'overview') {
      var updated = document.getElementById('last-live-update');
      var timestamp = window.strandLocalISO(now);
      if (updated && timestamp) {
        var time = updated.querySelector('time');
        setText(time, timestamp.slice(11, 19) + ' UTC+7');
        time.dateTime = new Date(now).toISOString();
        updated.hidden = false;
      }
    }
    if (type === 'snapshot') {
      if (j.server_time) { var st = Date.parse(j.server_time); if (!isNaN(st)) { window.strandSkew = st - now; } }
      if (j.overview) { applyOverview(j.overview); }
      (j.pipelines || []).forEach(function (r) { applyRecord(r, now); });
    } else if (type === 'sample') { applyRecord(j, now); }
    else if (type === 'overview') { applyOverview(j); }
    else if (type === 'structure') { structural(); }
    else if (type === 'log') { applyLog(j); }
    else if (type === 'bye') { location.href = '/login'; }
    paintPill();
  }
  var es = null, fails = 0, pollTimer = null;
  function streamURL() { return '/api/v1/stream?pipeline=' + encodeURIComponent(target) + (wantLogs ? '&logs=1' : ''); }
  function connect() {
    if (typeof window.EventSource !== 'function') { startPolling(); return; }
    es = new window.EventSource(streamURL());
    ['snapshot', 'sample', 'overview', 'structure', 'log', 'bye'].forEach(function (t) {
      es.addEventListener(t, function (ev) { fails = 0; handle(t, ev.data); });
    });
    es.onopen = function () { fails = 0; setConn('live'); };
    es.onerror = function () {
      fails++;
      setConn('reconnecting');
      // The browser reconnects by itself (sending Last-Event-ID). A 401 or a proxy that buffers the stream
      // makes it fail repeatedly: check the session, then fall back to polling.
      if (fails === 1 || es.readyState === 2) {
        fetch('/api/v1/session', { credentials: 'same-origin' }).then(function (r) { if (r.status === 401) { location.href = '/login'; } }).catch(function () { /* offline */ });
      }
      if (fails >= 3 || es.readyState === 2) { es.close(); es = null; startPolling(); }
    };
  }
  function startPolling() {
    if (pollTimer) { return; }
    setConn('polling');
    var poll = function () {
      fetch('/api/v1/live?pipeline=' + encodeURIComponent(target), { credentials: 'same-origin' })
        .then(function (r) {
          if (r.status === 401) { location.href = '/login'; return null; }
          return r.ok ? r.text() : null;
        })
        .then(function (t) { if (t) { setConn('polling'); handle('snapshot', t); } else { setConn('offline'); } })
        .catch(function () { setConn('offline'); });
    };
    poll();
    pollTimer = setInterval(poll, interval * 1000);
    // try the event stream again later
    if (typeof window.EventSource === 'function') {
      setTimeout(function () { clearInterval(pollTimer); pollTimer = null; fails = 0; connect(); }, 60000);
    }
  }
  // Polling the log tail is not part of /api/v1/live: without SSE the Logs tab keeps its rendered lines.

  logAttach();
  chartsLoad();
  connect();
  window.strandLive = { applyRecord: applyRecord, applyOverview: applyOverview, applyLog: applyLog, handle: handle, relAgo: relAgo };
})();
