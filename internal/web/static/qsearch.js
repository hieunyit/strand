// Quick search (Ctrl+K / Cmd+K): pipelines, backup jobs, every page of the navigation, and a restore point or
// backup job by its ID. Data is fetched when the dialog opens; nothing is sent while typing.
(function () {
  var dlg = document.getElementById('qsearch'), input = document.getElementById('qsearch-input'), list = document.getElementById('qsearch-results');
  var opener = document.getElementById('qsearch-open');
  if (!dlg || !input || !list || typeof dlg.showModal !== 'function') { return; }
  var items = [], shown = [], sel = 0, loadPending = null;
  var ID_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{2,127}$/;

  function navItems() {
    var out = [];
    Array.prototype.forEach.call(document.querySelectorAll('nav[aria-label="Main"] a[href]'), function (a) {
      var label = (a.textContent || '').trim();
      if (label) { out.push({ label: label, kind: a.classList.contains('sub') ? 'Pipeline' : 'Page', href: a.getAttribute('href') }); }
    });
    return out;
  }
  function load() {
    if (loadPending) { return loadPending; }
    items = navItems();
    if (!window.strandApi) { return Promise.resolve(); }
    var get = function (url) { return window.strandApi('GET', url).then(function (r) { return r.ok ? r.json() : {}; }).catch(function () { return {}; }); };
    loadPending = Promise.all([get('/api/v1/pipelines'), get('/api/v1/backup-jobs')]).then(function (res) {
      var seen = {};
      items.forEach(function (it) { seen[it.href] = true; });
      (res[0].pipelines || []).forEach(function (p) {
        var href = '/pipelines/' + encodeURIComponent(p.name);
        if (!seen[href]) { items.push({ label: p.name, kind: 'Pipeline', detail: (p.tags || []).join(' '), href: href }); seen[href] = true; }
      });
      (res[1].jobs || []).forEach(function (v) {
        var j = v.job || v;
        items.push({ label: j.name || j.id, kind: 'Backup job', detail: j.id, href: '/backups/jobs/' + encodeURIComponent(j.id) });
      });
    });
    return loadPending;
  }
  function score(it, q) {
    var l = (it.label + ' ' + (it.detail || '') + ' ' + it.kind).toLowerCase();
    if (l.indexOf(q) === 0) { return 3; }
    if ((' ' + l).indexOf(' ' + q) >= 0) { return 2; }
    return l.indexOf(q) >= 0 ? 1 : 0;
  }
  function render() {
    var q = input.value.trim().toLowerCase();
    shown = !q ? items.slice(0, 12) : items.map(function (it) { return { it: it, s: score(it, q) }; })
      .filter(function (x) { return x.s > 0; }).sort(function (a, b) { return b.s - a.s; }).slice(0, 12).map(function (x) { return x.it; });
    var raw = input.value.trim();
    if (raw && ID_RE.test(raw)) {
      shown.push({ label: 'Open restore point ' + raw, kind: 'Backup ID', href: '/backups/catalog/' + encodeURIComponent(raw) });
    }
    if (sel >= shown.length) { sel = Math.max(0, shown.length - 1); }
    list.textContent = '';
    shown.forEach(function (it, i) {
      var li = document.createElement('li'), a = document.createElement('a');
      li.setAttribute('role', 'option'); li.id = 'qs-opt-' + i;
      li.setAttribute('aria-selected', i === sel ? 'true' : 'false');
      a.href = it.href; a.tabIndex = -1;
      var k = document.createElement('span'); k.className = 'qs-kind'; k.textContent = it.kind;
      var l = document.createElement('span'); l.className = 'qs-label'; l.textContent = it.label;
      a.appendChild(l); a.appendChild(k);
      li.appendChild(a); list.appendChild(li);
    });
    if (!shown.length) {
      var li = document.createElement('li'); li.className = 'qs-empty'; li.textContent = 'No match. Try a pipeline or job name, a page, or a backup ID.';
      list.appendChild(li);
    }
    input.setAttribute('aria-activedescendant', shown.length ? 'qs-opt-' + sel : '');
  }
  function open() {
    if (dlg.open) { return; }
    input.value = ''; sel = 0;
    dlg.showModal();
    input.focus();
    render();
    load().then(render);
  }
  function go(i) { var it = shown[i]; if (it) { dlg.close(); window.location.href = it.href; } }
  document.addEventListener('keydown', function (e) {
    if ((e.ctrlKey || e.metaKey) && !e.altKey && (e.key === 'k' || e.key === 'K')) { e.preventDefault(); open(); }
  });
  if (opener) { opener.addEventListener('click', open); }
  input.addEventListener('input', function () { sel = 0; render(); });
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      dlg.close();
      if (opener) { opener.focus(); }
    } else if (e.key === 'ArrowDown') { e.preventDefault(); sel = Math.min(sel + 1, shown.length - 1); render(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); sel = Math.max(sel - 1, 0); render(); }
    else if (e.key === 'Enter') { e.preventDefault(); go(sel); }
  });
  list.addEventListener('click', function (e) {
    var li = e.target.closest ? e.target.closest('li[role="option"]') : null;
    if (li) { e.preventDefault(); go(Number(li.id.replace('qs-opt-', ''))); }
  });
  dlg.addEventListener('click', function (e) { if (e.target === dlg) { dlg.close(); } }); // backdrop
  window.strandQuickSearch = { open: open, items: function () { return items; }, shown: function () { return shown; } };
})();
