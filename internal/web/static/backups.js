(function () {
  'use strict';
  if (!window.strandApi || !document.getElementById('backup-job-form')) { return; }
  var jobID = '';
  function form() { return document.getElementById('backup-job-form'); }
  function field(name) { return form().elements.namedItem(name); }
  function message() { return document.getElementById('backup-job-message'); }
  function showError(err) { if (message()) { message().textContent = err || 'Request failed'; } }
  function readJob() {
    var minutes = Number(field('interval_minutes').value);
    return {
      name: field('name').value.trim(), command: field('command').value.trim(),
      every_seconds: minutes * 60, artifact_template: field('artifact_template').value.trim(),
      manifest_template: field('manifest_template').value.trim(), bucket: field('bucket').value.trim(),
      prefix: field('prefix').value.trim(), enabled: field('enabled').checked,
      tunnel_ssh: field('tunnel_ssh').value.trim(), tunnel_user: field('tunnel_user').value.trim(),
      tunnel_remote: field('tunnel_remote').value.trim(), tunnel_identity_file: field('tunnel_identity_file').value.trim(),
      tunnel_known_hosts: field('tunnel_known_hosts').value.trim(), cleanup_local: field('cleanup_local').checked
    };
  }
  function fill(job) {
    ['name','command','artifact_template','manifest_template','bucket','prefix','tunnel_ssh','tunnel_user','tunnel_remote','tunnel_identity_file','tunnel_known_hosts'].forEach(function (name) {
      field(name).value = job[name] || '';
    });
    field('interval_minutes').value = Math.max(1, Math.floor(job.every_seconds / 60));
    field('enabled').checked = !!job.enabled;
    field('cleanup_local').checked = !!job.cleanup_local;
    jobID = job.id;
    document.getElementById('backup-job-form-title').textContent = 'Edit backup job';
    document.getElementById('backup-job-save').textContent = 'Save changes';
    document.getElementById('backup-job-cancel').hidden = false;
    form().setAttribute('data-dirty', '1');
    form().scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  document.addEventListener('click', function (event) {
    var cancel = event.target.closest('#backup-job-cancel');
    if (cancel) {
      form().reset(); jobID = '';
      document.getElementById('backup-job-form-title').textContent = 'Create backup job';
      document.getElementById('backup-job-save').textContent = 'Create job';
      cancel.hidden = true; message().textContent = ''; form().removeAttribute('data-dirty');
      return;
    }
    var button = event.target.closest('[data-backup-job-action]');
    if (!button) { return; }
    var row = button.closest('[data-backup-job-id]');
    var id = row && row.getAttribute('data-backup-job-id');
    if (!id) { return; }
    var action = button.getAttribute('data-backup-job-action');
    if (action === 'edit') {
      window.strandApi('GET', '/api/v1/backup-jobs/' + encodeURIComponent(id)).then(function (resp) {
        return resp.json().then(function (body) { if (!resp.ok) { throw new Error(body.error || 'Could not load job'); } return body.job; });
      }).then(fill).catch(function (err) { window.alert(err.message); });
      return;
    }
    var prompts = { start: 'Start this scheduled backup job? The first backup runs immediately.', stop: 'Pause this backup job?', delete: 'Delete this backup job definition? Existing backups will remain stored.', run: 'Run one backup now? It waits for any run of this job that is already in progress.' };
    if (!window.confirm(prompts[action])) { return; }
    var method = action === 'delete' ? 'DELETE' : 'POST';
    var url = '/api/v1/backup-jobs/' + encodeURIComponent(id) + (action === 'delete' ? '' : '/' + action);
    window.strandApi(method, url).then(function (resp) {
      if (!resp.ok) { return resp.json().then(function (body) { throw new Error(body.error || 'Request failed'); }); }
      location.reload();
    }).catch(function (err) { window.alert(err.message); });
  });
  document.addEventListener('submit', function (event) {
    if (!event.target.matches('#backup-job-form')) { return; }
    event.preventDefault(); message().textContent = '';
    var payload = readJob();
    var method = jobID ? 'PUT' : 'POST';
    var url = '/api/v1/backup-jobs' + (jobID ? '/' + encodeURIComponent(jobID) : '');
    window.strandApi(method, url, payload).then(function (resp) {
      return resp.json().then(function (body) { if (!resp.ok) { throw new Error(body.error || 'Could not save backup job'); } return body; });
    }).then(function () { location.reload(); }).catch(function (err) { showError(err.message); });
  });
}());
