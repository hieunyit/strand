// Strand web console: periodic refresh of the server-rendered content. No libraries.
(function () {
  'use strict';
  var content = document.getElementById('content');
  if (!content) { return; }
  var csrfMeta = document.querySelector('meta[name="csrf-token"]');
  var csrf = csrfMeta ? csrfMeta.getAttribute('content') : '';
  var seconds = parseInt(content.getAttribute('data-refresh') || '0', 10);
  var busy = false;

  // Same-origin fetch that always carries the session cookie and the CSRF header (for POST).
  function api(method, url, body) {
    var opts = { method: method, credentials: 'same-origin', headers: { 'X-CSRF-Token': csrf } };
    if (body !== undefined) {
      opts.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    return fetch(url, opts);
  }
  window.strandApi = api; // extension point for M3 actions

  function refresh() {
    if (busy || document.hidden) { return; }
    busy = true;
    fetch(location.href, { credentials: 'same-origin', headers: { 'X-Requested-With': 'fetch' } })
      .then(function (r) {
        if (r.redirected && /\/login/.test(r.url)) { location.href = '/login'; return null; }
        if (r.status === 401) { location.href = '/login'; return null; }
        return r.ok ? r.text() : null;
      })
      .then(function (html) {
        if (!html) { return; }
        var doc = new DOMParser().parseFromString(html, 'text/html');
        var fresh = doc.getElementById('content');
        if (fresh) { content.innerHTML = fresh.innerHTML; }
      })
      .catch(function () { /* transient error: keep the old content */ })
      .then(function () { busy = false; });
  }

  if (seconds > 0) { setInterval(refresh, seconds * 1000); }
})();
