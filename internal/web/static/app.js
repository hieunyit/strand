// Strand web console. Live numbers arrive over Server-Sent Events and are updated in place (data-bind);
// the whole content is re-fetched only for structural changes. No libraries.
(function () {
  'use strict';
  var content = document.getElementById('content');
  if (!content) { return; }
  var csrfMeta = document.querySelector('meta[name="csrf-token"]');
  var csrf = csrfMeta ? csrfMeta.getAttribute('content') : '';
  var seconds = parseInt(content.getAttribute('data-refresh') || '0', 10);
  var busy = false;
  var onSwap = []; // called after a structural refresh replaced #content (the live module re-attaches)

  // Same-origin fetch that always carries the session cookie and the CSRF header (for POST).
  function api(method, url, body) {
    var opts = { method: method, credentials: 'same-origin', headers: { 'X-CSRF-Token': csrf } };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    return fetch(url, opts);
  }
  window.strandApi = api;

  // A refresh must not wipe a form the operator is typing in or a dialog that is open.
  function editing() {
    var a = document.activeElement;
    if (a && content.contains(a) && /^(INPUT|SELECT|TEXTAREA)$/.test(a.tagName)) { return true; }
    if (document.querySelector('dialog[open]')) { return true; }
    var cfgEdit = document.getElementById('cfg-edit-mode');
    if (cfgEdit && !cfgEdit.hidden) { return true; }
    var envEdit = document.getElementById('env-edit-mode');
    if (envEdit && !envEdit.hidden) { return true; }
    return !!content.querySelector('form[data-dirty]');
  }

  // Trạng thái giao diện phải giữ qua mỗi lần tự làm mới (thay #content): vị trí cuộn của trang và của các
  // khung cuộn (log, danh sách bảng...), <details> đang mở, ô tích / giá trị người dùng đã đổi. Không có bước
  // này, mỗi 5 giây bảng và log "nhảy" về đầu, mục mở bị đóng, lựa chọn bị mất.
  function keyOf(el, i) { return el.id ? '#' + el.id : el.tagName + ':' + i; }
  function scrollables() {
    var out = [];
    content.querySelectorAll('*').forEach(function (el) {
      if (el.scrollHeight > el.clientHeight + 1 || el.scrollWidth > el.clientWidth + 1) {
        var cs = getComputedStyle(el);
        if (/(auto|scroll)/.test(cs.overflow + cs.overflowY + cs.overflowX)) { out.push(el); }
      }
    });
    return out;
  }
  function saveState() {
    var st = { y: window.scrollY, x: window.scrollX, scroll: {}, open: {}, checks: {}, values: {} };
    scrollables().forEach(function (el, i) {
      st.scroll[keyOf(el, i)] = { top: el.scrollTop, left: el.scrollLeft, bottom: el.scrollTop + el.clientHeight >= el.scrollHeight - 4 };
    });
    content.querySelectorAll('details').forEach(function (el, i) { st.open[keyOf(el, i)] = el.open; });
    content.querySelectorAll('input[name], select[name], textarea[name]').forEach(function (el) {
      var k = (el.form && el.form.getAttribute('data-action') || '') + '|' + el.name;
      if (el.type === 'checkbox' || el.type === 'radio') {
        if (el.checked !== el.defaultChecked) { st.checks[k + '|' + el.value] = el.checked; }
      } else if (el.type !== 'hidden' && el.type !== 'password' && el.value !== el.defaultValue) {
        st.values[k] = el.value;
      }
    });
    return st;
  }
  function restoreState(st) {
    content.querySelectorAll('details').forEach(function (el, i) {
      var k = keyOf(el, i);
      if (k in st.open) { el.open = st.open[k]; }
    });
    content.querySelectorAll('input[name], select[name], textarea[name]').forEach(function (el) {
      var k = (el.form && el.form.getAttribute('data-action') || '') + '|' + el.name;
      if (el.type === 'checkbox' || el.type === 'radio') {
        if ((k + '|' + el.value) in st.checks) { el.checked = st.checks[k + '|' + el.value]; }
      } else if (k in st.values) {
        el.value = st.values[k];
      }
    });
    scrollables().forEach(function (el, i) {
      var s = st.scroll[keyOf(el, i)];
      if (!s) { return; }
      // Đang ở cuối (log đang chạy) thì bám cuối; không thì giữ đúng vị trí cũ.
      el.scrollTop = s.bottom && el.classList.contains('log') ? el.scrollHeight : s.top;
      el.scrollLeft = s.left;
    });
    window.scrollTo(st.x, st.y);
  }
  // Người dùng đang bôi đen chữ trong nội dung: không thay DOM (mất vùng chọn).
  function selecting() {
    var sel = window.getSelection && window.getSelection();
    return !!(sel && !sel.isCollapsed && sel.anchorNode && content.contains(sel.anchorNode));
  }

  var seq = 0, lastFresh = null;
  function refresh(force) {
    if (busy || document.hidden || (force !== true && (editing() || selecting()))) { return; }
    busy = true;
    var mine = ++seq, where = location.pathname + location.search;
    fetch(location.href, { credentials: 'same-origin', headers: { 'X-Requested-With': 'fetch' } })
      .then(function (r) {
        if (r.redirected && /\/login/.test(r.url)) { location.href = '/login'; return null; }
        if (r.status === 401) { location.href = '/login'; return null; }
        // Server chuyển hướng sang trang khác: không đổ nội dung trang khác vào tab hiện tại.
        if (r.redirected && new URL(r.url).pathname !== location.pathname) { return null; }
        return r.ok ? r.text() : null;
      })
      .then(function (html) {
        // Bỏ kết quả cũ: đã có lần làm mới mới hơn, hoặc người dùng đã sang tab/trang khác.
        if (!html || mine !== seq || where !== location.pathname + location.search) { return; }
        var doc = new DOMParser().parseFromString(html, 'text/html');
        var fresh = doc.getElementById('content');
        if (!fresh || fresh.innerHTML === lastFresh) { return; }
        // A proxy may serve another tab's HTML without an HTTP redirect. Bind
        // the server's active tab to the requested path before replacing content.
        var activeTab = fresh.querySelector('nav.tabs a.active');
        if (activeTab && new URL(activeTab.getAttribute('href'), location.href).pathname !== location.pathname) { return; }
        if (force !== true && (editing() || selecting())) { return; }
        var st = saveState();
        content.innerHTML = fresh.innerHTML;
        lastFresh = fresh.innerHTML;
        restoreState(st);
        applyList();
        applyTables();
        onSwap.forEach(function (f) { f(); });
      })
      .catch(function () {
        // Transient error: keep the old content but say that it is stale.
        var a = document.getElementById('asof');
        if (a && !a.classList.contains('stale')) { a.classList.add('stale'); a.appendChild(document.createTextNode(' \u00b7 refresh failed, showing older data')); }
      })
      .then(function () { busy = false; });
  }

  // ---- list tables (pipelines, jobs): client-side search, engine/kind, state and pipeline filters, sort.
  // The filters live in the query string (shareable links) and survive the periodic refresh. ----
  var FILTERS = [['q', 'pl-filter'], ['kind', 'pl-kind'], ['state', 'pl-state'], ['pipeline', 'pl-pipeline']];
  var listState = { sort: '', dir: 1, q: '', kind: '', state: '', pipeline: '' };
  (function () {
    var qs = new URLSearchParams(location.search);
    FILTERS.forEach(function (f) { if (qs.get(f[0])) { listState[f[0]] = qs.get(f[0]); } });
    var s = qs.get('sort');
    if (s === 'lag' || s === 'rate') { listState.sort = s; listState.dir = -1; }
    else if (s === 'name' || s === 'sev') { listState.sort = s; listState.dir = 1; }
  })();
  function listTable() { return document.querySelector('#pipeline-table, table[data-filterable]'); }
  // Mirror the active filters into the address bar (no new history entry).
  function syncListURL() {
    if (!window.history || !history.replaceState) { return; }
    var qs = new URLSearchParams();
    FILTERS.forEach(function (f) { if (listState[f[0]]) { qs.set(f[0], listState[f[0]]); } });
    if (listState.sort) { qs.set('sort', listState.sort); }
    var str = qs.toString();
    try { history.replaceState(null, '', location.pathname + (str ? '?' + str : '')); } catch (e) { /* sandboxed frame */ }
  }
  function applyList() {
    var table = listTable();
    if (!table) { return; }
    var body = table.tBodies[0];
    var rows = Array.prototype.slice.call(body.querySelectorAll('tr[data-name]'));
    FILTERS.forEach(function (f) {
      var el = document.getElementById(f[1]);
      if (el && el.value !== listState[f[0]]) { el.value = listState[f[0]]; }
      if (el && el.tagName === 'SELECT') { el.classList.toggle('is-set', !!listState[f[0]]); }
    });
    if (listState.sort) {
      var key = listState.sort, dir = listState.dir;
      rows.sort(function (a, b) {
        var x, y;
        if (key === 'name') { x = a.getAttribute('data-name'); y = b.getAttribute('data-name'); return dir * x.localeCompare(y); }
        x = parseFloat(a.getAttribute('data-' + key)); y = parseFloat(b.getAttribute('data-' + key));
        if (x !== y) { return dir * (x - y); }
        return a.getAttribute('data-name').localeCompare(b.getAttribute('data-name'));
      });
      var anchor = body.querySelector('tr.nomatch');
      rows.forEach(function (r) { body.insertBefore(r, anchor); });
    }
    var q = listState.q.trim().toLowerCase(), shown = 0;
    rows.forEach(function (r) {
      var kinds = (r.getAttribute('data-kinds') || '').split(/\s+/);
      var ok = (!listState.state || r.getAttribute('data-state') === listState.state) &&
        (!listState.kind || kinds.indexOf(listState.kind) >= 0) &&
        (!listState.pipeline || r.getAttribute('data-pipeline') === listState.pipeline) &&
        (!q || r.textContent.toLowerCase().indexOf(q) >= 0);
      r.hidden = !ok;
      if (ok) { shown++; }
    });
    var nm = body.querySelector('tr.nomatch');
    if (nm) { nm.hidden = !(rows.length > 0 && shown === 0); }
    var active = !!(listState.q || listState.kind || listState.state || listState.pipeline);
    var cnt = document.getElementById('pl-count');
    var noun = table.id === 'jobs-table' ? ' jobs' : ' pipelines';
    if (cnt) { cnt.textContent = active ? shown + ' of ' + rows.length + ' shown' : rows.length + noun; }
    var clr = document.getElementById('pl-clear');
    if (clr) { clr.hidden = !active; }
    Array.prototype.forEach.call(table.querySelectorAll('th[aria-sort]'), function (th) {
      var b = th.querySelector('.sortbtn');
      th.setAttribute('aria-sort', b && b.getAttribute('data-sort') === listState.sort ? (listState.dir > 0 ? 'ascending' : 'descending') : 'none');
    });
    document.dispatchEvent(new CustomEvent('strand:list-applied'));
  }
  document.addEventListener('click', function (ev) {
    var c = ev.target && ev.target.closest && ev.target.closest('#pl-clear');
    if (c) {
      FILTERS.forEach(function (f) { listState[f[0]] = ''; });
      applyList(); syncListURL();
      return;
    }
    var b = ev.target && ev.target.closest && ev.target.closest('.sortbtn');
    if (!b || !listTable() || !listTable().contains(b)) { return; }
    var k = b.getAttribute('data-sort');
    if (listState.sort === k) { listState.dir = -listState.dir; }
    else { listState.sort = k; listState.dir = (k === 'lag' || k === 'rate') ? -1 : 1; }
    applyList(); syncListURL();
  });
  document.addEventListener('input', function (ev) {
    if (ev.target && ev.target.id === 'pl-filter') { listState.q = ev.target.value; applyList(); syncListURL(); }
  });
  document.addEventListener('change', function (ev) {
    var id = ev.target && ev.target.id;
    FILTERS.forEach(function (f) { if (id === f[1] && id !== 'pl-filter') { listState[f[0]] = ev.target.value; applyList(); syncListURL(); } });
  });
  applyList();

  // ---- Tables tab: status chips, search and sort; survive the periodic refresh (the state lives here, not in the DOM) ----
  var tblState = { f: 'all', q: '', sort: 'ord' };
  function applyTables() {
    var table = document.getElementById('tables-table');
    if (!table) { return; }
    // Paged list (many tables): the server filters, sorts and pages; nothing to do here.
    if (table.hasAttribute('data-paged')) { return; }
    var body = table.tBodies[0];
    var rows = Array.prototype.slice.call(body.querySelectorAll('tr[data-table]'));
    var qEl = document.getElementById('tb-filter'), sEl = document.getElementById('tb-sort');
    if (qEl && qEl.value !== tblState.q) { qEl.value = tblState.q; }
    if (sEl && sEl.value !== tblState.sort) { sEl.value = tblState.sort; }
    var key = tblState.sort, desc = key === 'pct' || key === 'rows';
    rows.sort(function (a, b) {
      var x, y;
      if (key === 'name') { return a.getAttribute('data-table').localeCompare(b.getAttribute('data-table')); }
      x = parseFloat(a.getAttribute('data-' + key)); y = parseFloat(b.getAttribute('data-' + key));
      if (key === 'eta') { x = x < 0 ? Infinity : x; y = y < 0 ? Infinity : y; }
      if (x !== y) { return desc ? y - x : (x < y ? -1 : 1); }
      return parseFloat(a.getAttribute('data-ord')) - parseFloat(b.getAttribute('data-ord'));
    });
    var anchor = body.querySelector('tr.nomatch');
    rows.forEach(function (r) { body.insertBefore(r, anchor); });
    var q = tblState.q.trim().toLowerCase(), shown = 0;
    rows.forEach(function (r) {
      var ok = (tblState.f === 'all' || r.getAttribute('data-state') === tblState.f) &&
        (!q || r.getAttribute('data-table').toLowerCase().indexOf(q) >= 0);
      r.hidden = !ok;
      if (ok) { shown++; }
    });
    var nm = body.querySelector('tr.nomatch');
    if (nm) { nm.hidden = !(rows.length > 0 && shown === 0); }
    var cnt = document.getElementById('tb-count');
    if (cnt) { cnt.textContent = (tblState.f !== 'all' || q) ? shown + ' of ' + rows.length + ' shown' : rows.length + ' tables'; }
    Array.prototype.forEach.call(document.querySelectorAll('[data-tfilter]'), function (c) {
      c.setAttribute('aria-pressed', c.getAttribute('data-tfilter') === tblState.f ? 'true' : 'false');
    });
  }
  document.addEventListener('click', function (ev) {
    var t = ev.target && ev.target.closest ? ev.target : null;
    if (!t) { return; }
    var chip = t.closest('[data-tfilter]');
    if (chip) {
      var f = chip.getAttribute('data-tfilter');
      tblState.f = (tblState.f === f && f !== 'all') ? 'all' : f; // a second click on the active chip clears it
      applyTables();
    } else if (t.closest('#tb-clear')) {
      tblState.f = 'all'; tblState.q = ''; applyTables();
    }
  });
  document.addEventListener('input', function (ev) {
    if (ev.target && ev.target.id === 'tb-filter') { tblState.q = ev.target.value; applyTables(); }
  });
  document.addEventListener('change', function (ev) {
    if (ev.target && ev.target.id === 'tb-sort') { tblState.sort = ev.target.value; applyTables(); }
    if (ev.target && ev.target.id === 'tb-psort' && ev.target.form) { ev.target.form.submit(); }
  });
  applyTables();

  // ---- colour theme: auto (system) -> light -> dark; stored in localStorage; theme.js applies it before first paint ----
  var themeBtn = document.getElementById('theme-toggle');
  function currentTheme() {
    var t = null;
    try { t = localStorage.getItem('strand-theme'); } catch (e) { t = null; }
    return t === 'light' || t === 'dark' ? t : 'auto';
  }
  function paintTheme() {
    var t = currentTheme();
    if (t === 'auto') { document.documentElement.removeAttribute('data-theme'); } else { document.documentElement.setAttribute('data-theme', t); }
    if (!themeBtn) { return; }
    var label = { auto: 'Auto', light: 'Light', dark: 'Dark' }[t];
    var use = themeBtn.querySelector('use'), txt = themeBtn.querySelector('.tlabel');
    if (use) { use.setAttribute('href', '/static/icons.svg#i-' + { auto: 'auto', light: 'sun', dark: 'moon' }[t]); }
    if (txt) { txt.textContent = label; }
    themeBtn.setAttribute('aria-label', 'Colour theme: ' + label.toLowerCase() + (t === 'auto' ? ' (follows the system)' : '') + '. Click to change.');
  }
  if (themeBtn) {
    themeBtn.addEventListener('click', function () {
      var next = { auto: 'light', light: 'dark', dark: 'auto' }[currentTheme()];
      try { if (next === 'auto') { localStorage.removeItem('strand-theme'); } else { localStorage.setItem('strand-theme', next); } } catch (e) { /* storage blocked */ }
      paintTheme();
    });
  }
  paintTheme();

  // User menu: close on outside click or Escape.
  document.addEventListener('click', function (ev) {
    var um = document.getElementById('usermenu');
    if (um && um.open && !um.contains(ev.target)) { um.open = false; }
  });
  // Row overflow menus (<details class="rowmenu">): one open at a time; close on outside click or Escape.
  document.addEventListener('click', function (ev) {
    document.querySelectorAll('details.rowmenu[open]').forEach(function (d) { if (!d.contains(ev.target)) { d.open = false; } });
  });
  document.addEventListener('keydown', function (ev) {
    if (ev.key !== 'Escape') { return; }
    document.querySelectorAll('details.rowmenu[open]').forEach(function (d) { d.open = false; var s = d.querySelector('summary'); if (s && d.contains(document.activeElement)) { s.focus(); } });
  });
  document.addEventListener('keydown', function (ev) {
    var um = document.getElementById('usermenu');
    if (ev.key === 'Escape' && um && um.open) { um.open = false; var s = um.querySelector('summary'); if (s) { s.focus(); } }
  });

  // Arrow keys / Home / End move between the page sections (tabs).
  document.addEventListener('keydown', function (ev) {
    var el = ev.target;
    if (!el || !el.closest) { return; }
    var tabNav = el.closest('nav.tabs, .tabs');
    if (!tabNav) { return; }
    if (el.tagName !== 'A' && el.getAttribute('role') !== 'tab' && !el.classList.contains('tab')) { return; }
    var links = Array.prototype.slice.call(tabNav.querySelectorAll('a, button, [role="tab"], .tab'));
    var i = links.indexOf(el), n = -1;
    if (ev.key === 'ArrowRight') { n = (i + 1) % links.length; }
    else if (ev.key === 'ArrowLeft') { n = (i - 1 + links.length) % links.length; }
    else if (ev.key === 'Home') { n = 0; }
    else if (ev.key === 'End') { n = links.length - 1; }
    if (n >= 0) {
      ev.preventDefault();
      links[n].focus();
      if (links[n].tagName === 'BUTTON' || (links[n].hasAttribute('role') && links[n].getAttribute('role') === 'tab' && !links[n].href)) {
        links[n].click();
      }
    }
  });

  // Client-side in-page tab switching for tabs specifying data-tab-target
  document.addEventListener('click', function (ev) {
    var tab = ev.target.closest('nav.tabs [data-tab-target]');
    if (!tab) return;
    var target = tab.getAttribute('data-tab-target');
    if (!target) return;
    ev.preventDefault();
    var nav = tab.closest('nav.tabs');
    if (nav) {
      nav.querySelectorAll('[data-tab-target]').forEach(function (t) {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
      });
      tab.classList.add('active');
      tab.setAttribute('aria-selected', 'true');
    }
    document.querySelectorAll('[data-tab-content]').forEach(function (section) {
      if (section.getAttribute('data-tab-content') === target) {
        section.hidden = false;
      } else {
        section.hidden = true;
      }
    });
  });

  if (seconds > 0) { setInterval(refresh, seconds * 1000); }
  // Hooks for /static/live.js (the SSE client): structural refresh, re-attach after a swap, table filters.
  window.strandConsole = { refresh: refresh, onSwap: onSwap, applyTables: applyTables, applyList: applyList, editing: editing, selecting: selecting, toast: toast, confirm: confirmDialog };

  // Helper that renders standard API {error, fields} into an inline alert + field errors.
  function renderApiErrors(container, res) {
    if (!container) return;
    var oldAlert = container.querySelector('.alert-error');
    if (oldAlert) { oldAlert.remove(); }
    container.querySelectorAll('.field-error').forEach(function (el) {
      el.textContent = '';
      el.hidden = true;
    });
    if (!res) return;
    var errText = res.error || (typeof res === 'string' ? res : '');
    if (errText) {
      var alert = document.createElement('div');
      alert.className = 'alert alert-error';
      alert.setAttribute('role', 'alert');
      alert.textContent = errText;
      container.insertBefore(alert, container.firstChild);
    }
    if (res.fields && typeof res.fields === 'object') {
      Object.keys(res.fields).forEach(function (field) {
        var input = container.querySelector('[name="' + field + '"]');
        if (input) {
          var fieldWrap = input.closest('.field, .form-field') || input.parentElement;
          var errSpan = fieldWrap ? fieldWrap.querySelector('.field-error') : null;
          if (!errSpan && fieldWrap) {
            errSpan = document.createElement('span');
            errSpan.className = 'field-error';
            fieldWrap.appendChild(errSpan);
          }
          if (errSpan) {
            errSpan.textContent = res.fields[field];
            errSpan.hidden = false;
          }
        }
      });
    }
  }
  window.strandRenderApiErrors = renderApiErrors;

  // ---- toast and confirmation dialog (no inline script: everything is wired here) ----
  var toastEl = document.getElementById('toast');
  var toastTimer = null;
  // Success toasts hide after 4 s; an error stays until it is dismissed (button, click or Esc).
  function toast(msg, ok) {
    if (!toastEl) { return; }
    toastEl.textContent = msg;
    toastEl.className = 'toast ' + (ok ? 'ok' : 'bad');
    toastEl.setAttribute('role', ok ? 'status' : 'alert');
    var x = document.createElement('button');
    x.type = 'button'; x.className = 'iconbtn toast-close'; x.setAttribute('aria-label', 'Dismiss notification');
    x.innerHTML = '<svg class="ic" aria-hidden="true" focusable="false"><use href="/static/icons.svg#i-close"/></svg>';
    x.addEventListener('click', function (ev) { ev.stopPropagation(); toastEl.hidden = true; clearTimeout(toastTimer); });
    toastEl.appendChild(x);
    toastEl.hidden = false;
    clearTimeout(toastTimer);
    if (ok) { toastTimer = setTimeout(function () { toastEl.hidden = true; }, 4000); }
  }

  if (toastEl) { toastEl.addEventListener('click', function () { if (toastEl.classList.contains('ok')) { toastEl.hidden = true; } }); }
  document.addEventListener('keydown', function (ev) {
    if (ev.key === 'Escape' && toastEl && !toastEl.hidden && !document.querySelector('dialog[open]')) { toastEl.hidden = true; }
  });
  var dlg = document.getElementById('confirm');
  function confirmDialog(message, retype, opts) {
    opts = opts || {};
    return new Promise(function (resolve) {
      if (!dlg || typeof dlg.showModal !== 'function') { resolve(window.confirm(message)); return; }
      var msg = document.getElementById('confirm-msg');
      var box = document.getElementById('confirm-retype');
      var nameEl = document.getElementById('confirm-name');
      var input = document.getElementById('confirm-input');
      var ok = document.getElementById('confirm-ok');
      var cancel = document.getElementById('confirm-cancel');
      var form = document.getElementById('confirm-form');
      var opener = document.activeElement;
      var titleEl = document.getElementById('confirm-title');
      if (titleEl) { titleEl.textContent = opts.title || (retype ? 'Confirm: this cannot be undone' : 'Confirm'); }
      ok.textContent = opts.ok || 'Confirm';
      ok.classList.toggle('danger', !!retype || !!opts.danger);
      msg.textContent = message;
      input.value = '';
      box.hidden = !retype;
      nameEl.textContent = retype || '';
      ok.disabled = !!retype;
      function onInput() { ok.disabled = !!retype && input.value !== retype; }
      function onSubmit(e) { if (retype && input.value !== retype) { e.preventDefault(); } }
      function onCancel() { dlg.close('cancel'); }
      // Keep Tab inside the dialog (showModal also makes the page inert; this covers browsers that let focus reach the UI).
      function onKey(e) {
        if (e.key !== 'Tab') { return; }
        var f = Array.prototype.filter.call(dlg.querySelectorAll('button, input, [href], select, textarea'), function (x) { return !x.disabled && !x.hidden && !x.closest('[hidden]'); });
        if (!f.length) { return; }
        var first = f[0], last = f[f.length - 1];
        if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
      }
      function onClose() {
        dlg.removeEventListener('keydown', onKey);
        if (opener && opener.focus && document.contains(opener)) { try { opener.focus(); } catch (e) { /* element gone */ } }
        input.removeEventListener('input', onInput);
        form.removeEventListener('submit', onSubmit);
        cancel.removeEventListener('click', onCancel);
        dlg.removeEventListener('close', onClose);
        resolve(dlg.returnValue === 'ok' ? (retype ? input.value : true) : false);
      }
      input.addEventListener('input', onInput);
      form.addEventListener('submit', onSubmit);
      cancel.addEventListener('click', onCancel);
      dlg.addEventListener('close', onClose);
      dlg.addEventListener('keydown', onKey);
      dlg.returnValue = '';
      dlg.showModal();
      if (retype) { input.focus(); } else { ok.focus(); }
    });
  }

  window.strandConfirm = confirmDialog;
  window.strandToast = toast;

  // Forms with class "act": data-action, data-pipeline, optional data-confirm and data-retype (pipeline name).
  function collect(form) {
    var body = {};
    Array.prototype.forEach.call(form.elements, function (el) {
      if (!el.name || el.disabled) { return; }
      if (el.type === 'checkbox' || el.type === 'radio') {
        if (!el.checked) { return; }
      }
      var v = (el.value || '').trim();
      if (v === '') { return; }
      body[el.name] = body[el.name] ? body[el.name] + ',' + v : v;
    });
    return body;
  }

  function runAction(form, body, action) {
    action = action || form.getAttribute('data-action');
    var url = '/api/v1/pipelines/' + encodeURIComponent(form.getAttribute('data-pipeline')) + '/actions/' + encodeURIComponent(action);
    var buttons = Array.prototype.filter.call(form.querySelectorAll('button'), function (b) { return !b.disabled; });
    Array.prototype.forEach.call(buttons, function (b) { b.disabled = true; });
    form.setAttribute('aria-busy', 'true');
    toast('Running ' + action + '...', true);
    api('POST', url, body).then(function (r) {
      return r.json().then(function (j) { return { status: r.status, j: j }; }, function () { return { status: r.status, j: {} }; });
    }).then(function (res) {
      if (res.status === 401) { location.href = '/login'; return; }
      if (res.status >= 200 && res.status < 300) {
        form.removeAttribute('data-dirty');
        if (res.j.job_id && !form.hasAttribute('data-stay')) { location.href = '/jobs/' + res.j.job_id; return; }
        toast(action + ': ' + (res.j.message || 'ok'), true);
        if (action === 'delete') {
          setTimeout(function () { location.href = '/'; }, 1000);
          return;
        }
        setTimeout(function () { refresh(true); }, 1500);
      } else {
        renderApiErrors(form, res.j);
        toast(action + ' failed (' + res.status + '): ' + (res.j.error || 'error'), false);
      }
    }).catch(function (e) {
      toast(action + ' failed: ' + e, false);
    }).then(function () {
      form.removeAttribute('aria-busy');
      Array.prototype.forEach.call(buttons, function (b) { b.disabled = false; });
    });
  }

  document.addEventListener('submit', function (ev) {
    var form = ev.target;
    if (!form || !form.classList || !form.classList.contains('act')) { return; }
    if (form.id === 'new-pipeline-form' || !form.getAttribute('data-pipeline')) { return; }
    ev.preventDefault();
    var body = collect(form);
    // A form may have several submit buttons, each its own action (data-do) with its own dialogs.
    var src = ev.submitter && ev.submitter.hasAttribute('data-do') ? ev.submitter : form;
    var act = src === form ? null : src.getAttribute('data-do');
    var message = src.getAttribute('data-confirm');
    if (!message) { runAction(form, body, act); return; }
    var retype = src.getAttribute('data-retype') || '';
    confirmDialog(message, retype).then(function (res) {
      if (!res) { return; }
      if (retype) { body.confirm_name = retype; }
      // Failover: a second, separate dialog that must be retyped again.
      var msg2 = src.getAttribute('data-confirm2');
      if (!msg2) { runAction(form, body, act); return; }
      confirmDialog(msg2, retype).then(function (res2) {
        if (!res2) { return; }
        body.confirm_name2 = retype;
        runAction(form, body, act);
      });
    });
  });

  document.addEventListener('input', function (ev) {
    var form = ev.target && ev.target.form;
    if (form) { form.setAttribute('data-dirty', '1'); }
  });
  document.addEventListener('change', function (ev) {
    var form = ev.target && ev.target.form;
    if (form) { form.setAttribute('data-dirty', '1'); }
  });

  // ---- live job output: poll the offset endpoint until the job ends ----
  var out = document.getElementById('joboutput');
  if (out) {
    var jobId = out.getAttribute('data-job');
    var offset = parseInt(out.getAttribute('data-offset') || '0', 10);
    var wasRunning = out.getAttribute('data-running') === 'true';
    var poll = function () {
      fetch('/api/v1/jobs/' + encodeURIComponent(jobId) + '/output?offset=' + offset, { credentials: 'same-origin' })
        .then(function (r) { if (r.status === 401) { location.href = '/login'; return null; } return r.ok ? r.json() : null; })
        .then(function (j) {
          if (!j) { setTimeout(poll, 3000); return; }
          if (j.data) {
            var stick = out.scrollTop + out.clientHeight >= out.scrollHeight - 20;
            out.textContent += j.data;
            if (stick) { out.scrollTop = out.scrollHeight; }
          }
          offset = j.offset;
          if (j.running) { setTimeout(poll, 1000); } else if (wasRunning) { location.reload(); }
        })
        .catch(function () { setTimeout(poll, 3000); });
    };
    if (wasRunning) { setTimeout(poll, 800); }
  }

  // ---- Config editing and .env (M5) ----
  document.addEventListener('click', function (ev) {
    var t = ev.target;
    if (!t) return;
    if (t.id === 'cfg-btn-toggle') {
      var vm = document.getElementById('cfg-view-mode');
      var em = document.getElementById('cfg-edit-mode');
      var tb = document.getElementById('cfg-btn-toggle');
      if (vm && em) {
        vm.hidden = true;
        em.hidden = false;
        if (tb) tb.hidden = true;
        var form = document.getElementById('cfg-edit-form');
        if (form) form.setAttribute('data-dirty', '1');
      }
    } else if (t.id === 'cfg-btn-cancel') {
      var vm = document.getElementById('cfg-view-mode');
      var em = document.getElementById('cfg-edit-mode');
      var tb = document.getElementById('cfg-btn-toggle');
      if (vm && em) {
        vm.hidden = false;
        em.hidden = true;
        if (tb) tb.hidden = false;
        var form = document.getElementById('cfg-edit-form');
        if (form) form.removeAttribute('data-dirty');
      }
    } else if (t.id === 'cfg-btn-diff') {
      var form = document.getElementById('cfg-edit-form');
      var ta = document.getElementById('cfg-yaml-textarea');
      var p = form ? form.getAttribute('data-pipeline') : '';
      if (!p || !ta) return;
      toast('Validating configuration...', true);
      api('POST', '/api/v1/pipelines/' + encodeURIComponent(p) + '/config/validate', { yaml: ta.value })
        .then(function (r) { return r.json(); })
        .then(function (res) {
          var box = document.getElementById('cfg-diff-box');
          var st = document.getElementById('cfg-val-status');
          var out = document.getElementById('cfg-diff-output');
          if (box && st && out) {
            box.hidden = false;
            st.innerHTML = res.valid
              ? '<span class="badge st-synced">Validation PASS</span>'
              : '<span class="badge st-failed">Validation FAIL (exit ' + res.exit_code + ')</span> ' + (res.output || '');
            out.textContent = res.diff || '(no changes detected)';
          }
          toast(res.valid ? 'Validation passed' : 'Validation failed', res.valid);
        })
        .catch(function (err) { toast('Validate error: ' + err, false); });
    } else if (t.id === 'cfg-btn-save') {
      var form = document.getElementById('cfg-edit-form');
      var ta = document.getElementById('cfg-yaml-textarea');
      var restart = document.getElementById('cfg-restart-check');
      var force = document.getElementById('cfg-force-check');
      var p = form ? form.getAttribute('data-pipeline') : '';
      if (!p || !ta) return;
      var rst = restart && restart.checked;
      var frc = force && force.checked;
      toast('Saving configuration...', true);
      api('POST', '/api/v1/pipelines/' + encodeURIComponent(p) + '/config', { yaml: ta.value, restart: rst, force: frc })
        .then(function (r) {
          return r.json().then(function (j) { return { ok: r.ok, j: j }; });
        })
        .then(function (res) {
          if (res.ok) {
            toast(res.j.message || ('Configuration saved!' + (res.j.restarted ? ' Pipeline restarted.' : '')), true);
            setTimeout(function () { location.reload(); }, 1200);
          } else {
            var box = document.getElementById('cfg-diff-box');
            var st = document.getElementById('cfg-val-status');
            var out = document.getElementById('cfg-diff-output');
            if (box && st && out) {
              box.hidden = false;
              st.innerHTML = '<span class="badge st-failed">Save rejected: validation failed</span>';
              out.textContent = res.j.error || 'Validation failed. Check "Save anyway" to bypass and save.';
              if (force) { force.focus(); }
            }
            toast('Save failed: ' + (res.j.error || 'error'), false);
          }
        })
        .catch(function (err) { toast('Save error: ' + err, false); });
    } else if (t.id === 'tuning-btn-diff' || t.id === 'tuning-btn-save') {
      var tform = document.getElementById('tuning-form');
      var tp = tform ? tform.getAttribute('data-pipeline') : '';
      if (!tp) return;
      var dry = t.id === 'tuning-btn-diff';
      var body = {
        dry_run: dry,
        restart: !dry && !!(document.getElementById('tuning-restart') || {}).checked,
        force: !dry && !!(document.getElementById('tuning-force') || {}).checked
      };
      Array.prototype.forEach.call(tform.querySelectorAll('[data-tuning]'), function (el) {
        body[el.getAttribute('data-tuning')] = el.value.trim(); // rỗng = bỏ khóa (về mặc định)
      });
      var dbox = document.getElementById('tuning-diff-output');
      toast(dry ? 'Validating...' : 'Saving tuning...', true);
      api('POST', '/api/v1/pipelines/' + encodeURIComponent(tp) + '/tuning', body)
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
        .then(function (res) {
          if (!res.ok) {
            toast((res.j.error || 'error'), false);
            if (dbox) { dbox.hidden = true; }
            return;
          }
          if (dbox) { dbox.hidden = false; dbox.textContent = res.j.diff || '(no changes)'; }
          if (dry) {
            toast(res.j.valid === false ? 'Validation failed: ' + (res.j.output || '') : 'Validation passed', res.j.valid !== false);
          } else {
            toast(res.j.message || 'Saved', true);
            if (res.j.changed && res.j.changed.length) setTimeout(function () { location.reload(); }, 1500);
          }
        })
        .catch(function (err) { toast('Tuning error: ' + err, false); });
    } else if (t.id === 'env-btn-save') {
      var form = document.getElementById('env-edit-form');
      var p = form ? form.getAttribute('data-pipeline') : '';
      if (!p) return;
      var entries = {};
      var inputs = form.querySelectorAll('input[data-key]');
      Array.prototype.forEach.call(inputs, function (inp) {
        var k = inp.getAttribute('data-key');
        var v = inp.value;
        entries[k] = v;
      });
      var newK = document.getElementById('env-new-key');
      var newV = document.getElementById('env-new-val');
      if (newK && newV && newK.value.trim()) {
        entries[newK.value.trim()] = newV.value;
      }
      toast('Saving .env...', true);
      api('POST', '/api/v1/pipelines/' + encodeURIComponent(p) + '/env', { entries: entries })
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
        .then(function (res) {
          if (res.ok) {
            toast('.env saved successfully!', true);
            setTimeout(function () { location.reload(); }, 1200);
          } else {
            toast('.env save failed: ' + (res.j.error || 'error'), false);
          }
        })
        .catch(function (err) { toast('.env error: ' + err, false); });
    }
  });

  // Config editor: "Upload YAML" replaces the editor text (nothing is saved until Save), then previews the diff.
  document.addEventListener('change', function (ev) {
    var inp = ev.target;
    if (!inp || inp.id !== 'cfg-upload' || !inp.files || !inp.files[0]) { return; }
    var f = inp.files[0];
    if (f.size > 1048576) { toast('Upload failed: the file is larger than 1 MiB', false); inp.value = ''; return; }
    f.text().then(function (txt) {
      var ta = document.getElementById('cfg-yaml-textarea');
      if (!ta) { return; }
      ta.value = txt;
      var form = document.getElementById('cfg-edit-form');
      if (form) { form.setAttribute('data-dirty', '1'); }
      toast('Loaded ' + f.name + ': review the diff, then Save', true);
      var b = document.getElementById('cfg-btn-diff');
      if (b) { b.click(); }
    }, function (e) { toast('Upload failed: ' + e, false); });
    inp.value = '';
  });

  // ---- New pipeline form handler (M5) ----
  function escHtml(x) {
    return String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  // Form tạo pipeline: chỉ hiện ô hợp với cặp engine; ô bị ẩn thì disabled nên không được gửi.
  function npSetup(form) {
    var from = document.getElementById('np-from'), to = document.getElementById('np-to');
    if (!from || !to) return;
    function engineOf(v) { return v === 'postgres' ? 'postgres' : (v === 'mongodb' ? 'mongodb' : 'mysql'); }
    function matches(cond, src, dst) {
      var kv = cond.split(':'), vals = kv[1].split(',');
      return vals.indexOf(kv[0] === 'src' ? src : dst) >= 0;
    }
    function setPort(el, eng) {
      if (el && !el.dataset.touched && el.dataset[eng]) el.value = el.dataset[eng];
    }
    ['np-src-port', 'np-dst-port'].forEach(function (id) {
      var el = document.getElementById(id);
      if (el) el.addEventListener('input', function () { el.dataset.touched = '1'; });
    });
    function val(name) {
      var el = form.querySelector('[name="' + name + '"]:not([disabled])');
      return el ? el.value.trim() : '';
    }
    function preview() {
      var src = from.value, out = document.getElementById('np-preview');
      if (!out) return;
      var inc = val('include'), exc = val('exclude'), what;
      if (src === 'mongodb') {
        what = (inc || ((val('source_database') || '?') + '.* (every collection)')) + (exc ? '  except ' + exc : '') +
          '  ->  MongoDB database ' + (val('target_mongo_database') || '?');
        out.textContent = what;
        return;
      }
      if (inc) {
        what = inc;
      } else if (src === 'postgres') {
        what = (val('schema') || 'public') + '.* (every table in schema ' + (val('schema') || 'public') + ' of database ' + (val('database') || '?') + ')';
      } else {
        var dbs = val('database').split(',').map(function (x) { return x.trim(); }).filter(Boolean);
        what = dbs.length ? dbs.map(function (d) { return d === '*' ? '*.* (all databases)' : d + '.*'; }).join(', ') : '(enter a database)';
      }
      if (exc) what += '  except ' + exc;
      var dst = to.value;
      if (dst === 'postgres') {
        var tdb = val('target_pg_database') || (src === 'postgres' ? val('database') : (val('database').split(',')[0] || '').trim());
        what += (tdb && tdb !== '*') ? '  ->  PostgreSQL database ' + tdb : '  ->  PostgreSQL: enter the Target database in step 3';
      } else {
        what += '  ->  MySQL (same database names; checkpoint in ' + (val('target_database') || 'strand') + ')';
      }
      out.textContent = what;
    }
    function apply() {
      var src = from.value;
      // Cặp được hỗ trợ: PG chỉ sang PG, MariaDB chỉ sang MySQL.
      var firstOK = null;
      Array.prototype.forEach.call(to.options, function (o) {
        var ok = (o.dataset.from || '').split(' ').indexOf(src) >= 0;
        o.disabled = !ok; o.hidden = !ok;
        if (ok && !firstOK) firstOK = o;
      });
      if (to.selectedOptions[0] && to.selectedOptions[0].disabled && firstOK) to.value = firstOK.value;
      var note = document.getElementById('np-pair-note');
      if (note) note.textContent = src === 'postgres' ? 'PostgreSQL can only be copied to PostgreSQL.' : (src === 'mariadb' ? 'MariaDB can only be copied to MySQL.' : (src === 'mongodb' ? 'MongoDB can only be copied to MongoDB.' : ''));
      var dst = to.value;
      form.querySelectorAll('[data-when]').forEach(function (el) {
        var on = matches(el.dataset.when, src, dst);
        el.hidden = !on;
        el.querySelectorAll('input, select, textarea').forEach(function (i) { i.disabled = !on; });
      });
      setPort(document.getElementById('np-src-port'), engineOf(src));
      setPort(document.getElementById('np-dst-port'), engineOf(dst));
      var ns = src === 'postgres' ? 'schema' : 'db';
      if (src === 'mongodb') ns = 'db';
      form.querySelectorAll('.np-ns').forEach(function (e) { e.textContent = ns; });
      var exInc = document.getElementById('np-include-ex'), exExc = document.getElementById('np-exclude-ex');
      var pre = src === 'postgres' ? (val('schema') || 'public') : (src === 'mongodb' ? (val('source_database') || 'app') : ((val('database').split(',')[0] || 'shop').trim().replace('*', 'shop')));
      if (exInc) exInc.textContent = pre + '.orders, ' + pre + '.customers';
      if (exExc) exExc.textContent = pre + '.log_*, ' + pre + '.tmp_*';
      preview();
    }
    from.addEventListener('change', apply);
    to.addEventListener('change', apply);
    form.addEventListener('input', function (ev) { if (ev.target !== from && ev.target !== to) apply(); });
    apply();
  }
  var newPipeForm = document.getElementById('new-pipeline-form');
  if (newPipeForm) {
    npSetup(newPipeForm);
    var listDBButton = document.getElementById('np-list-databases');
    if (listDBButton) listDBButton.addEventListener('click', function () {
      var status = document.getElementById('np-database-list-status');
      var box = document.getElementById('np-database-list');
      var field = document.getElementById('np-database');
      var value = function (name) { var el = newPipeForm.querySelector('[name="' + name + '"]'); return el ? el.value.trim() : ''; };
      listDBButton.disabled = true;
      if (status) status.textContent = ' Connecting to source…';
      api('POST', '/api/v1/source/databases', {from: value('from'), host: value('source_host'), port: Number(value('source_port')), user: value('source_user'), password: value('source_password')})
        .then(function (r) { return r.json().then(function (j) { if (!r.ok) throw new Error(j.error || 'Could not list databases'); return j; }); })
        .then(function (data) {
          if (!box || !field) return;
          box.innerHTML = '';
          if (!data.databases || !data.databases.length) { if (status) status.textContent = ' No accessible user databases found.'; box.hidden = true; return; }
          data.databases.forEach(function (name, index) {
            var label = document.createElement('label'); label.className = 'np-db-option';
            var cb = document.createElement('input'); cb.type = 'checkbox'; cb.value = name;
            cb.id = 'np-db-option-' + index;
            cb.checked = field.value.split(',').map(function (x) { return x.trim(); }).indexOf(name) >= 0;
            var text = document.createElement('span'); text.textContent = name;
            label.htmlFor = cb.id;
            label.appendChild(cb); label.appendChild(text);
            box.appendChild(label);
          });
          var actions = document.createElement('button'); actions.type = 'button'; actions.className = 'btn'; actions.textContent = 'Use selected databases';
          actions.addEventListener('click', function () {
            var selected = Array.prototype.map.call(box.querySelectorAll('input[type="checkbox"]:checked'), function (cb) { return cb.value; });
            field.value = selected.join(', '); field.dispatchEvent(new Event('input', {bubbles:true}));
            if (status) status.textContent = selected.length ? ' Applied ' + selected.length + ' database(s).' : ' Choose at least one database.';
          });
          box.appendChild(actions); box.hidden = false;
          if (status) status.textContent = ' Select one or more databases.';
        })
        .catch(function (err) { if (status) status.textContent = ' ' + err.message; })
        .finally(function () { listDBButton.disabled = false; });
    });
    newPipeForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      if (newPipeForm.getAttribute('aria-busy') === 'true') { return; } // a create is already running
      var wz = window.strandWizard;
      if (wz && !wz.validateAll()) { return; }
      var btn = document.getElementById('btn-create-pipeline');
      if (wz) { wz.busy(true); } else if (btn) { btn.disabled = true; }
      toast('Creating and validating pipeline...', true);
      var payload = {};
      var fd = new FormData(newPipeForm);
      fd.forEach(function (v, k) {
        var str = (v || '').trim();
        if (str !== '') {
          if (k === 'source_port' || k === 'target_port' || k === 'workers' || k === 'server_id' ||
              k === 'iops_soft' || k === 'iops_hard' || k === 'chunk_rows' || k === 'max_parallel') {
            payload[k] = parseInt(str, 10);
          } else if (k === 'breaker_max_ratio') {
            payload[k] = parseFloat(str);
          } else if (k === 'include' || k === 'exclude' || k === 'databases' || k === 'windows') {
            // API nhận mảng chuỗi: tách theo dấu phẩy
            payload[k] = str.split(',').map(function (x) { return x.trim(); }).filter(Boolean);
          } else if (k === 'overwrite') {
            payload[k] = v === 'true' || v === '1' || v === 'on';
          } else {
            payload[k] = str;
          }
        }
      });
      if (newPipeForm.getAttribute('data-allow-overwrite') === payload.name) {
        payload.overwrite = true;
      }
      api('POST', '/api/v1/pipelines', payload)
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, status: r.status, j: j }; }); })
        .then(function (res) {
          var resBox = document.getElementById('new-pipeline-result');
          var resBody = document.getElementById('result-body');
          if (resBox && resBody) {
            resBox.hidden = false;
            if (res.ok) {
              if (res.j.ok) {
                newPipeForm.removeAttribute('data-allow-overwrite');
                var owBox = document.getElementById('np-overwrite');
                if (owBox) owBox.checked = false;
              } else {
                newPipeForm.setAttribute('data-allow-overwrite', res.j.pipeline);
                var owBox = document.getElementById('np-overwrite');
                if (owBox) owBox.checked = true;
              }
              var vPass = res.j.validate && res.j.validate.exit_code === 0;
              var pPass = res.j.plan && res.j.plan.exit_code === 0;
              var html = '';
              if (!res.j.ok) {
                // Tạo file xong nhưng validate lỗi: báo rõ, không cho Start ngay.
                html += '<p class="err"><b>Validation failed.</b> ' + escHtml(res.j.message || '') + '</p>';
              }
              html += '<p><span class="badge ' + (vPass ? 'st-synced' : 'st-failed') + '">Validate: ' + (vPass ? 'PASS' : 'FAIL') + '</span> ';
              html += '<span class="badge ' + (pPass ? 'st-synced' : 'st-warn') + '">Plan: ' + (pPass ? 'PASS' : 'WARN') + '</span></p>';
              html += '<h4>Validate Output</h4><pre class="code">' + escHtml(res.j.validate ? res.j.validate.output : '') + '</pre>';
              html += '<h4>Plan Output</h4><pre class="code">' + escHtml(res.j.plan ? res.j.plan.output : '') + '</pre>';
              html += '<div class="mt14">';
              if (res.j.ok) {
                html += '<form class="act inline" data-action="start" data-pipeline="' + escHtml(res.j.pipeline) + '">';
                html += '<button class="btn primary" type="submit">Start ' + escHtml(res.j.pipeline) + ' now</button></form> ';
              }
              html += '<a href="/pipelines/' + encodeURIComponent(res.j.pipeline) + '" class="btn">' + (res.j.ok ? 'Go to pipeline' : 'Fix in Config tab') + '</a>';
              html += '</div>';
              resBody.innerHTML = html;
              newPipeForm.dispatchEvent(new CustomEvent('pipeline-created'));
              if (res.j.ok) { toast('Pipeline created successfully!', true); }
              else { toast('Pipeline created but validation FAILED: fix it before Start', false); }
            } else {
              var errHtml = '<p class="err">Error: ' + escHtml(res.j.error || 'Failed to create pipeline') + '</p>';
              if (res.status === 409) {
                errHtml += '<p class="mt10"><button type="button" class="btn warning" id="btn-force-overwrite">Overwrite &amp; recreate "' + escHtml(payload.name || '') + '"</button></p>';
              }
              resBody.innerHTML = errHtml;
              if (wz) { wz.showApiError(res.j); }
              var forceBtn = document.getElementById('btn-force-overwrite');
              if (forceBtn) {
                forceBtn.addEventListener('click', function () {
                  newPipeForm.setAttribute('data-allow-overwrite', payload.name);
                  var owBox = document.getElementById('np-overwrite');
                  if (owBox) owBox.checked = true;
                  btn.click();
                });
              }
              toast('Creation failed: ' + (res.j.error || 'error'), false);
            }
          }
        })
        .catch(function (err) { toast('Error: ' + err, false); })
        .then(function () { if (wz) { wz.busy(false); } else if (btn) { btn.disabled = false; } });
    });
    // A successful create leaves nothing to lose: drop the dirty mark so leaving the page does not ask.
    newPipeForm.addEventListener('pipeline-created', function () {
      newPipeForm.removeAttribute('data-dirty');
      newPipeForm.hidden = true;
      var stp = document.getElementById('np-stepper');
      if (stp) { stp.hidden = true; }
      var box = document.getElementById('new-pipeline-result');
      if (box && box.scrollIntoView) { box.scrollIntoView({ block: 'start' }); }
    });
  }

  // Unsaved changes: leaving a page with a dirty form[data-guard] asks first. The periodic refresh never
  // replaces such a form (editing() above).
  window.addEventListener('beforeunload', function (ev) {
    if (document.querySelector('form[data-guard][data-dirty]')) { ev.preventDefault(); ev.returnValue = ''; }
  });
})();
