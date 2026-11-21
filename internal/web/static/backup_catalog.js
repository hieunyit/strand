// Restore point (catalog) actions: verify, legal hold, copy to another repository and delete (restore points
// that are not complete only), on /backups/catalog and
// /backups/catalog/{id}. Every write goes through window.strandApi (session cookie + CSRF header); the server
// enforces RBAC. Restores open the restore wizard (/backups/restore?backup=<id>).
(function () {
  'use strict';
  if (!window.strandApi || !document.querySelector('[data-backup-action], #backup-copy-form')) { return; }
  var api = window.strandApi;

  function result(resp, fallback) {
    return resp.json().catch(function () { return {}; }).then(function (body) {
      if (!resp.ok) { throw new Error(body.error || fallback); }
      return body;
    });
  }
  function ask(text, ok) {
    if (window.strandConfirm) { return window.strandConfirm(text, '', { ok: ok }); }
    return Promise.resolve(window.confirm(text));
  }
  // Long operations (verify, copy) run as background processes: follow them on their job page.
  function follow(body) {
    if (body && body.process_id) { window.location.href = '/jobs/' + encodeURIComponent(body.process_id); return; }
    window.location.reload();
  }
  function fail(err) { window.alert(err.message); }

  document.addEventListener('click', function (ev) {
    var btn = ev.target.closest && ev.target.closest('[data-backup-action]');
    if (!btn) { return; }
    var row = btn.closest('[data-backup-id]');
    var id = btn.getAttribute('data-id') || (row && row.getAttribute('data-backup-id'));
    if (!id) { return; }
    var base = '/api/v1/backups/' + encodeURIComponent(id);
    var act = btn.getAttribute('data-backup-action');
    if (act === 'verify') {
      ask('Re-read backup ' + id + ' from the repository and check its checksums?', 'Verify').then(function (yes) {
        if (!yes) { return; }
        api('POST', base + '/verify').then(function (r) { return result(r, 'Could not start the verification'); }).then(follow).catch(fail);
      });
    } else if (act === 'delete') {
      // Restore points that are not complete (failed, expired): the confirm text comes translated from the page.
      var text = btn.getAttribute('data-confirm') || ('Delete restore point ' + id + '?');
      var asked = window.strandConfirm ? window.strandConfirm(text, '', { ok: btn.getAttribute('data-ok') || 'Delete', danger: true })
        : Promise.resolve(window.confirm(text));
      asked.then(function (yes) {
        if (!yes) { return; }
        btn.disabled = true;
        api('DELETE', base).then(function (r) { return result(r, 'Could not delete the restore point'); }).then(follow)
          .catch(function (err) { btn.disabled = false; fail(err); });
      });
    } else if (act === 'legal-hold' || act === 'hold') {
      var hold = btn.getAttribute('data-hold') === '1';
      ask(hold ? 'Put backup ' + id + ' on legal hold? Retention never deletes it while the hold is set.' : 'Release the legal hold of backup ' + id + '?',
        hold ? 'Set legal hold' : 'Release').then(function (yes) {
        if (!yes) { return; }
        api('POST', base + '/legal-hold', { hold: hold }).then(function (r) { return result(r, 'Could not change the legal hold'); })
          .then(function () { window.location.reload(); }).catch(fail);
      });
    }
  });

  var copyForm = document.getElementById('backup-copy-form');
  if (copyForm) {
    copyForm.addEventListener('submit', function (ev) {
      ev.preventDefault();
      var id = String(copyForm.elements.namedItem('backup_id').value || '').trim();
      var repo = String(copyForm.elements.namedItem('repository').value || '').trim();
      if (!repo) { return; }
      api('POST', '/api/v1/backups/' + encodeURIComponent(id) + '/copy', { repository: repo })
        .then(function (r) { return result(r, 'Could not start the copy'); })
        .then(function (body) {
          var d = document.getElementById('copy-drawer');
          if (d && d.close) { d.close(); }
          follow(body);
        }).catch(fail);
    });
  }
}());
