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
  window.strandApi = api;

  // A refresh must not wipe a form the operator is typing in or a dialog that is open.
  function editing() {
    var a = document.activeElement;
    if (a && content.contains(a) && /^(INPUT|SELECT|TEXTAREA)$/.test(a.tagName)) { return true; }
    if (document.querySelector('dialog[open]')) { return true; }
    return !!content.querySelector('form[data-dirty]');
  }

  function refresh(force) {
    if (busy || document.hidden || (force !== true && editing())) { return; }
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

  // ---- toast and confirmation dialog (no inline script: everything is wired here) ----
  var toastEl = document.getElementById('toast');
  var toastTimer = null;
  function toast(msg, ok) {
    if (!toastEl) { return; }
    toastEl.textContent = msg;
    toastEl.className = 'toast ' + (ok ? 'ok' : 'bad');
    toastEl.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { toastEl.hidden = true; }, ok ? 8000 : 20000);
  }

  var dlg = document.getElementById('confirm');
  function confirmDialog(message, retype) {
    return new Promise(function (resolve) {
      if (!dlg || typeof dlg.showModal !== 'function') { resolve(window.confirm(message)); return; }
      var msg = document.getElementById('confirm-msg');
      var box = document.getElementById('confirm-retype');
      var nameEl = document.getElementById('confirm-name');
      var input = document.getElementById('confirm-input');
      var ok = document.getElementById('confirm-ok');
      var cancel = document.getElementById('confirm-cancel');
      var form = document.getElementById('confirm-form');
      msg.textContent = message;
      input.value = '';
      box.hidden = !retype;
      nameEl.textContent = retype || '';
      ok.disabled = !!retype;
      function onInput() { ok.disabled = !!retype && input.value !== retype; }
      function onSubmit(e) { if (retype && input.value !== retype) { e.preventDefault(); } }
      function onCancel() { dlg.close('cancel'); }
      function onClose() {
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
      dlg.returnValue = '';
      dlg.showModal();
      if (retype) { input.focus(); }
    });
  }

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
    var buttons = form.querySelectorAll('button');
    Array.prototype.forEach.call(buttons, function (b) { b.disabled = true; });
    toast('Running ' + action + '...', true);
    api('POST', url, body).then(function (r) {
      return r.json().then(function (j) { return { status: r.status, j: j }; }, function () { return { status: r.status, j: {} }; });
    }).then(function (res) {
      if (res.status === 401) { location.href = '/login'; return; }
      if (res.status >= 200 && res.status < 300) {
        form.removeAttribute('data-dirty');
        if (res.j.job_id && !form.hasAttribute('data-stay')) { location.href = '/jobs/' + res.j.job_id; return; }
        toast(action + ': ' + (res.j.message || 'ok'), true);
        setTimeout(function () { refresh(true); }, 1500);
      } else {
        toast(action + ' failed (' + res.status + '): ' + (res.j.error || 'error'), false);
      }
    }).catch(function (e) {
      toast(action + ' failed: ' + e, false);
    }).then(function () {
      Array.prototype.forEach.call(buttons, function (b) { b.disabled = false; });
    });
  }

  document.addEventListener('submit', function (ev) {
    var form = ev.target;
    if (!form || !form.classList || !form.classList.contains('act')) { return; }
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
    if (form && form.classList && form.classList.contains('act')) { form.setAttribute('data-dirty', '1'); }
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
      if (vm && em) { vm.hidden = true; em.hidden = false; }
    } else if (t.id === 'cfg-btn-cancel') {
      var vm = document.getElementById('cfg-view-mode');
      var em = document.getElementById('cfg-edit-mode');
      if (vm && em) { vm.hidden = false; em.hidden = true; }
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
      var p = form ? form.getAttribute('data-pipeline') : '';
      if (!p || !ta) return;
      var rst = restart && restart.checked;
      toast('Saving configuration...', true);
      api('POST', '/api/v1/pipelines/' + encodeURIComponent(p) + '/config', { yaml: ta.value, restart: rst })
        .then(function (r) {
          return r.json().then(function (j) { return { ok: r.ok, j: j }; });
        })
        .then(function (res) {
          if (res.ok) {
            toast('Configuration saved!' + (res.j.restarted ? ' Pipeline restarted.' : ''), true);
            setTimeout(function () { location.reload(); }, 1200);
          } else {
            toast('Save failed: ' + (res.j.error || 'error'), false);
          }
        })
        .catch(function (err) { toast('Save error: ' + err, false); });
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

  // ---- New pipeline form handler (M5) ----
  function escHtml(x) {
    return String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  var newPipeForm = document.getElementById('new-pipeline-form');
  if (newPipeForm) {
    newPipeForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var btn = document.getElementById('btn-create-pipeline');
      if (btn) btn.disabled = true;
      toast('Creating and validating pipeline...', true);
      var payload = {};
      var fd = new FormData(newPipeForm);
      fd.forEach(function (v, k) {
        var str = (v || '').trim();
        if (str !== '') {
          if (k === 'source_port' || k === 'target_port' || k === 'workers') {
            payload[k] = parseInt(str, 10);
          } else if (k === 'include' || k === 'exclude' || k === 'databases') {
            // API nhận mảng chuỗi: tách theo dấu phẩy
            payload[k] = str.split(',').map(function (x) { return x.trim(); }).filter(Boolean);
          } else {
            payload[k] = str;
          }
        }
      });
      api('POST', '/api/v1/pipelines', payload)
        .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
        .then(function (res) {
          var resBox = document.getElementById('new-pipeline-result');
          var resBody = document.getElementById('result-body');
          if (resBox && resBody) {
            resBox.hidden = false;
            if (res.ok) {
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
              html += '<div style="margin-top:16px;">';
              if (res.j.ok) {
                html += '<form class="act inline" data-action="start" data-pipeline="' + escHtml(res.j.pipeline) + '">';
                html += '<button class="btn primary" type="submit">Start ' + escHtml(res.j.pipeline) + ' now</button></form> ';
              }
              html += '<a href="/pipelines/' + encodeURIComponent(res.j.pipeline) + '" class="btn">' + (res.j.ok ? 'Go to pipeline' : 'Fix in Config tab') + '</a>';
              html += '</div>';
              resBody.innerHTML = html;
              if (res.j.ok) { toast('Pipeline created successfully!', true); }
              else { toast('Pipeline created but validation FAILED: fix it before Start', false); }
            } else {
              resBody.innerHTML = '<p class="err">Error: ' + escHtml(res.j.error || 'Failed to create pipeline') + '</p>';
              toast('Creation failed: ' + (res.j.error || 'error'), false);
            }
          }
        })
        .catch(function (err) { toast('Error: ' + err, false); })
        .then(function () { if (btn) btn.disabled = false; });
    });
  }
})();
