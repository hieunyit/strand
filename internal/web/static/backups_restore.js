(function () {
  'use strict';

  var form = document.getElementById('restore-form');
  if (!form) { return; }

  var rawData = form.getAttribute('data-wizard');
  var wizardData = {};
  try {
    if (rawData) { wizardData = JSON.parse(rawData); }
  } catch (e) {
    console.error('Failed to parse wizard data', e);
  }

  var sources = wizardData.sources || [];
  var connections = wizardData.connections || [];
  var backups = wizardData.backups || [];

  var curStep = 1;
  var testOK = false;
  var existingDBs = [];
  var lastTestError = '';

  function normalizeTargetTime(tt) {
    if (!tt) { return ''; }
    var s = tt.trim();
    if (!s) { return ''; }
    if (s.endsWith('Z')) {
      s = s.slice(0, -1);
    }
    if (s.indexOf('.') !== -1) {
      s = s.split('.')[0];
    }
    if (s.length === 16) {
      s += ':00';
    }
    return s + 'Z';
  }

  function getChosenEngine() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    if (fromBackup) {
      var b = getChosenBackup();
      return b ? (b.engine || '') : '';
    }
    var src = (form.elements['source'].value || '').trim();
    var sInfo = sources.find(function (x) { return x.source === src; });
    return sInfo ? (sInfo.engine || '') : '';
  }

  function resetTestStatus() {
    testOK = false;
    existingDBs = [];
    lastTestError = '';
    var testResult = document.getElementById('restore-test-result');
    if (testResult) { testResult.hidden = true; testResult.textContent = ''; }
    var status = document.getElementById('restore-test-status');
    if (status) { status.textContent = ''; }
  }

  function filterConnectionsByEngine() {
    var eng = getChosenEngine();
    var sel = document.getElementById('restore-conn-select');
    if (!sel) { return; }
    var hadSelected = sel.value;
    var selectedStillValid = false;
    Array.prototype.forEach.call(sel.options, function (opt) {
      if (!opt.value || opt.value === 'custom') {
        opt.hidden = false;
        if (opt.value === hadSelected) { selectedStillValid = true; }
        return;
      }
      var optEng = opt.getAttribute('data-engine') || '';
      var match = (!eng || optEng === eng);
      opt.hidden = !match;
      if (opt.value === hadSelected && match) {
        selectedStillValid = true;
      }
    });
    if (hadSelected && !selectedStillValid) {
      sel.value = '';
      syncConnection();
    }
  }

  var stepperLis = document.querySelectorAll('#restore-stepper li[data-step-li]');
  var stepPanels = document.querySelectorAll('#restore-form .wiz-step[data-step]');

  function updateStepper(step) {
    curStep = step;
    stepperLis.forEach(function (li) {
      var s = parseInt(li.getAttribute('data-step-li'), 10);
      li.classList.remove('done', 'current');
      li.removeAttribute('aria-current');
      if (s < curStep) {
        li.classList.add('done');
      } else if (s === curStep) {
        li.classList.add('current');
        li.setAttribute('aria-current', 'step');
      }
    });

    stepPanels.forEach(function (panel) {
      var s = parseInt(panel.getAttribute('data-step'), 10);
      panel.hidden = (s !== curStep);
    });

    if (curStep === 4) {
      populateSummary();
      updateConfirm();
    }
  }

  // Radio switching for restore_from
  function syncRestoreFrom() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    var fBackup = form.querySelector('[data-show-if="restore_from=backup"]');
    var fPitr = form.querySelector('[data-show-if="restore_from=pitr"]');
    if (fBackup) { fBackup.hidden = !fromBackup; }
    if (fPitr) { fPitr.hidden = fromBackup; }
    var hint = document.getElementById('restore-time-hint');
    if (hint) {
      hint.textContent = fromBackup ? 'Optional: roll forward from backup snapshot with archived logs.' : 'Required for point-in-time recovery.';
    }
    syncPitrWindow();
    filterConnectionsByEngine();
  }

  // Radio switching for restore_into
  function syncRestoreInto() {
    var intoDb = form.elements['restore_into'].value === 'database';
    var fDb = document.getElementById('field-target-db');
    var fDir = document.getElementById('field-target-dir');
    if (fDb) { fDb.hidden = !intoDb; }
    if (fDir) { fDir.hidden = intoDb; }
  }

  // Target connection change
  function syncConnection() {
    var sel = document.getElementById('restore-conn-select');
    var customDiv = document.getElementById('restore-custom-conn');
    var isCustom = sel && sel.value === 'custom';
    if (customDiv) { customDiv.hidden = !isCustom; }
    testOK = false;
    existingDBs = [];
    var testResult = document.getElementById('restore-test-result');
    if (testResult) { testResult.hidden = true; testResult.textContent = ''; }
    var status = document.getElementById('restore-test-status');
    if (status) { status.textContent = ''; }

    // If backup tool was pg_basebackup, enforce restore_into = dir
    var chosenTool = getChosenTool();
    var choiceDir = document.getElementById('choice-restore-dir');
    var choiceDb = document.getElementById('choice-restore-db');
    var radDir = form.elements['restore_into'] && form.elements['restore_into'][1];
    var radDb = form.elements['restore_into'] && form.elements['restore_into'][0];

    if (chosenTool === 'pg_basebackup') {
      if (radDir) { radDir.checked = true; }
      if (choiceDb) { choiceDb.hidden = true; }
    } else {
      if (choiceDb) { choiceDb.hidden = false; }
    }
    syncRestoreInto();
  }

  function getChosenTool() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    if (fromBackup) {
      var bid = (form.elements['backup_id'].value || '').trim();
      var b = backups.find(function (x) { return x.id === bid; });
      return b ? b.tool : '';
    }
    return '';
  }

  function getChosenBackup() {
    var bid = (form.elements['backup_id'].value || '').trim();
    return backups.find(function (x) { return x.id === bid; });
  }

  function syncPitrWindow() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    var winBox = document.getElementById('restore-source-window');
    var winText = document.getElementById('restore-window-text');
    var preview = document.getElementById('restore-pitr-preview');
    var timeInput = document.getElementById('restore-target-time');

    var src = '';
    if (!fromBackup) {
      src = (form.elements['source'].value || '').trim();
    } else {
      var b = getChosenBackup();
      if (b) { src = b.source; }
    }

    if (!src) {
      if (winBox) { winBox.hidden = true; }
      if (timeInput) { timeInput.removeAttribute('min'); timeInput.removeAttribute('max'); }
      return;
    }

    var sInfo = sources.find(function (x) { return x.source === src; });
    if (!sInfo) {
      if (winBox) { winBox.hidden = true; }
      return;
    }

    if (sInfo.window_start && sInfo.window_end) {
      var wStart = new Date(sInfo.window_start);
      var wEnd = new Date(sInfo.window_end);
      if (timeInput) {
        timeInput.min = wStart.toISOString().slice(0, 19);
        timeInput.max = wEnd.toISOString().slice(0, 19);
      }
      if (winBox && winText) {
        winBox.hidden = false;
        winText.textContent = 'Recovery window for ' + src + ': ' +
          wStart.toISOString().slice(0, 19).replace('T', ' ') + ' to ' +
          wEnd.toISOString().slice(0, 19).replace('T', ' ') +
          ' UTC (' + (sInfo.logs_count || 0) + ' log segments available).';
      }
    } else {
      if (winBox) { winBox.hidden = true; }
      if (timeInput) { timeInput.removeAttribute('min'); timeInput.removeAttribute('max'); }
    }

    // PITR preview
    if (preview && timeInput && timeInput.value) {
      var normVal = normalizeTargetTime(timeInput.value);
      var tVal = new Date(normVal);
      var base = (sInfo.base_backups || []).filter(function (bk) {
        return new Date(bk.created_at) <= tVal;
      }).pop();
      if (base) {
        preview.textContent = 'Restore plan: base backup ' + base.id + ' (' +
          new Date(base.created_at).toISOString().slice(0, 16).replace('T', ' ') + ') rolled forward to target time.';
      } else {
        preview.textContent = 'No base backup found before target time.';
      }
    } else if (preview) {
      preview.textContent = '';
    }
  }

  // Validate step 1
  function validateStep1() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    var errBackup = document.getElementById('err-backup-id');
    var errSource = document.getElementById('err-source');
    var errTime = document.getElementById('err-target-time');
    if (errBackup) { errBackup.hidden = true; }
    if (errSource) { errSource.hidden = true; }
    if (errTime) { errTime.hidden = true; }

    var valid = true;
    if (fromBackup) {
      var bid = (form.elements['backup_id'].value || '').trim();
      if (!bid) {
        if (errBackup) { errBackup.textContent = 'Enter or select a backup ID.'; errBackup.hidden = false; }
        valid = false;
      }
    } else {
      var src = (form.elements['source'].value || '').trim();
      if (!src) {
        if (errSource) { errSource.textContent = 'Select a source to restore from.'; errSource.hidden = false; }
        valid = false;
      } else {
        var sInfoContinuous = sources.find(function (x) { return x.source === src; });
        if (sInfoContinuous && sInfoContinuous.continuous === false) {
          if (errSource) {
            errSource.textContent = 'Source ' + src + ' has no continuous PITR window.';
            errSource.hidden = false;
          }
          valid = false;
        }
      }
      var tt = (form.elements['target_time'].value || '').trim();
      if (!tt) {
        if (errTime) { errTime.textContent = 'Target point in time is required for PITR.'; errTime.hidden = false; }
        valid = false;
      } else {
        var sInfo = sources.find(function (x) { return x.source === src; });
        if (sInfo && sInfo.window_start && sInfo.window_end) {
          var tSel = new Date(normalizeTargetTime(tt));
          var tMin = new Date(sInfo.window_start);
          var tMax = new Date(sInfo.window_end);
          if (tSel < tMin || tSel > tMax) {
            if (errTime) {
              errTime.textContent = 'Target time is outside the recovery window [' +
                tMin.toISOString().slice(0, 19).replace('T', ' ') + ', ' +
                tMax.toISOString().slice(0, 19).replace('T', ' ') + '].';
              errTime.hidden = false;
            }
            valid = false;
          }
        }
      }
    }
    return valid;
  }

  // Validate step 2
  function validateStep2() {
    var errConn = document.getElementById('err-connection');
    var errTest = document.getElementById('err-test-connection');
    var errDb = document.getElementById('err-target-database');
    var errDir = document.getElementById('err-target-dir');
    if (errConn) { errConn.hidden = true; }
    if (errTest) { errTest.hidden = true; }
    if (errDb) { errDb.hidden = true; }
    if (errDir) { errDir.hidden = true; }

    var connSel = (form.elements['connection_id'].value || '').trim();
    if (!connSel) {
      if (errConn) { errConn.textContent = 'Choose a target connection or select manual settings.'; errConn.hidden = false; }
      return false;
    }
    if (connSel === 'custom') {
      var host = (form.elements['target_host'].value || '').trim();
      if (!host) {
        if (errConn) { errConn.textContent = 'Target host is required for manual connection.'; errConn.hidden = false; }
        return false;
      }
    }

    if (!testOK) {
      if (errTest) {
        errTest.textContent = lastTestError || 'Test the connection successfully before continuing.';
        errTest.hidden = false;
      }
      return false;
    }

    var intoDir = form.elements['restore_into'].value === 'dir';
    if (!intoDir) {
      var dbName = (form.elements['target_database'].value || '').trim();
      if (!dbName) {
        if (errDb) { errDb.textContent = 'Enter a new database name.'; errDb.hidden = false; }
        return false;
      }
      if (existingDBs && existingDBs.indexOf(dbName) !== -1) {
        if (errDb) {
          errDb.textContent = 'Database "' + dbName + '" already exists on target. Restore requires a NEW database.';
          errDb.hidden = false;
        }
        return false;
      }
    } else {
      var dirName = (form.elements['target_dir'].value || '').trim();
      if (!dirName) {
        if (errDir) { errDir.textContent = 'Enter a new data directory.'; errDir.hidden = false; }
        return false;
      }
    }
    return true;
  }

  // Test connection button
  var testBtn = document.getElementById('restore-test-btn');
  if (testBtn) {
    testBtn.addEventListener('click', function () {
      var connSel = (form.elements['connection_id'].value || '').trim();
      var errTest = document.getElementById('err-test-connection');
      if (errTest) { errTest.hidden = true; }
      var status = document.getElementById('restore-test-status');
      var testResult = document.getElementById('restore-test-result');

      if (!connSel) {
        if (errTest) { errTest.textContent = 'Select a connection to test.'; errTest.hidden = false; }
        return;
      }

      if (status) { status.textContent = 'Testing connection...'; }

      var p;
      if (connSel !== 'custom') {
        p = window.strandApi('POST', '/api/v1/backup-connections/' + encodeURIComponent(connSel) + '/test');
      } else {
        var chosenEng = getChosenEngine();
        var payload = {
          name: 'Manual connection',
          engine: chosenEng,
          host: (form.elements['target_host'].value || '').trim(),
          port: parseInt(form.elements['target_port'].value, 10) || 0,
          user: (form.elements['target_user'].value || '').trim(),
          secret_mode: 'env',
          password_env: (form.elements['target_password_env'].value || '').trim()
        };
        p = window.strandApi('POST', '/api/v1/backup-connections/test', payload);
      }

      p.then(function (res) {
        return res.json().then(function (data) {
          return { ok: res.ok, status: res.status, data: data };
        });
      }).then(function (result) {
        if (!result.ok || !result.data || result.data.ok !== true) {
          testOK = false;
          existingDBs = [];
          if (status) { status.textContent = ''; }
          var errMsg = (result.data && result.data.error) || 'Connection test failed';
          if (result.data && result.data.checks && Array.isArray(result.data.checks)) {
            for (var i = 0; i < result.data.checks.length; i++) {
              if (result.data.checks[i].status === 'fail') {
                errMsg = result.data.checks[i].detail || result.data.checks[i].name || errMsg;
                break;
              }
            }
          }
          lastTestError = errMsg;
          if (errTest) {
            errTest.textContent = errMsg;
            errTest.hidden = false;
          }
          if (testResult) {
            testResult.hidden = true;
            testResult.textContent = '';
          }
          return;
        }
        var data = result.data;
        testOK = true;
        lastTestError = '';
        existingDBs = data.databases || [];
        if (status) { status.textContent = 'Connection test successful'; }
        if (testResult) {
          testResult.hidden = false;
          testResult.textContent = 'Connected successfully (' + (data.version || 'OK') + '). Existing databases on target: ' +
            (existingDBs.length > 0 ? existingDBs.join(', ') : 'none') + '.';
        }
      }).catch(function (err) {
        testOK = false;
        existingDBs = [];
        if (status) { status.textContent = ''; }
        if (errTest) { errTest.textContent = err.message; errTest.hidden = false; }
        if (testResult) { testResult.hidden = true; testResult.textContent = ''; }
      });
    });
  }

  function populateSummary() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    var itemEl = document.getElementById('summary-item');
    var timeEl = document.getElementById('summary-time');
    var srvEl = document.getElementById('summary-target-server');
    var destEl = document.getElementById('summary-target-dest');
    var sizeEl = document.getElementById('summary-size');

    if (fromBackup) {
      var bid = (form.elements['backup_id'].value || '').trim();
      var b = getChosenBackup();
      if (itemEl) { itemEl.textContent = 'Backup ' + bid + (b ? ' (' + b.source + ', ' + b.tool + ')' : ''); }
      if (sizeEl) { sizeEl.textContent = b ? formatBytes(b.bytes) : 'Unknown'; }
    } else {
      var src = (form.elements['source'].value || '').trim();
      if (itemEl) { itemEl.textContent = 'PITR from source: ' + src; }
      if (sizeEl) { sizeEl.textContent = 'Computed from base backup + logs'; }
    }

    var tt = (form.elements['target_time'].value || '').trim();
    if (timeEl) { timeEl.textContent = tt ? normalizeTargetTime(tt) + ' (UTC)' : 'Original backup snapshot'; }

    var connSel = (form.elements['connection_id'].value || '').trim();
    if (srvEl) {
      if (connSel !== 'custom') {
        var c = connections.find(function (x) { return x.id === connSel; });
        srvEl.textContent = c ? c.name + ' (' + c.host + ':' + c.port + ')' : connSel;
      } else {
        var host = (form.elements['target_host'].value || '').trim();
        var port = (form.elements['target_port'].value || '').trim();
        srvEl.textContent = host + (port ? ':' + port : '');
      }
    }

    var intoDir = form.elements['restore_into'].value === 'dir';
    if (destEl) {
      if (intoDir) {
        destEl.textContent = 'Directory: ' + (form.elements['target_dir'].value || '').trim();
      } else {
        destEl.textContent = 'Database: ' + (form.elements['target_database'].value || '').trim();
      }
    }
  }

  function updateConfirm() {
    var intoDir = form.elements['restore_into'].value === 'dir';
    var targetName = intoDir ? (form.elements['target_dir'].value || '').trim() : (form.elements['target_database'].value || '').trim();
    var confirmInput = document.getElementById('restore-confirm-input');
    var confirmVal = confirmInput ? confirmInput.value.trim() : '';
    var startBtn = document.getElementById('start-restore-btn');
    if (startBtn) {
      startBtn.disabled = !(targetName.length > 0 && confirmVal === targetName);
    }
    var hint = document.getElementById('restore-confirm-hint');
    if (hint) {
      hint.textContent = 'Type "' + targetName + '" to enable Start restore.';
    }
  }

  function formatBytes(bytes) {
    if (!bytes || bytes <= 0) { return '0 B'; }
    var k = 1024;
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  // Stepper click handlers
  document.addEventListener('click', function (ev) {
    var nextBtn = ev.target.closest('[data-next-step]');
    if (nextBtn) {
      var nextS = parseInt(nextBtn.getAttribute('data-next-step'), 10);
      if (curStep === 1 && !validateStep1()) { return; }
      if (curStep === 2 && !validateStep2()) { return; }
      updateStepper(nextS);
      return;
    }

    var prevBtn = ev.target.closest('[data-prev-step]');
    if (prevBtn) {
      var prevS = parseInt(prevBtn.getAttribute('data-prev-step'), 10);
      updateStepper(prevS);
      return;
    }

    var gotoBtn = ev.target.closest('[data-goto]');
    if (gotoBtn) {
      var targetS = parseInt(gotoBtn.getAttribute('data-goto'), 10);
      if (targetS > curStep) {
        if (curStep === 1 && !validateStep1()) { return; }
        if (curStep === 2 && !validateStep2()) { return; }
      }
      updateStepper(targetS);
      return;
    }
  });

  // Event listeners for inputs
  form.addEventListener('change', function (ev) {
    if (ev.target.name === 'restore_from') { syncRestoreFrom(); filterConnectionsByEngine(); }
    if (ev.target.name === 'restore_into') { syncRestoreInto(); }
    if (ev.target.name === 'connection_id') { syncConnection(); }
    if (ev.target.name === 'source' || ev.target.name === 'backup_id') { syncPitrWindow(); filterConnectionsByEngine(); }
  });

  form.addEventListener('input', function (ev) {
    if (ev.target.id === 'restore-confirm-input') { updateConfirm(); }
    if (ev.target.id === 'restore-target-database' || ev.target.id === 'restore-target-dir') { updateConfirm(); }
    if (ev.target.id === 'restore-target-time') { syncPitrWindow(); }
  });

  ['target_host', 'target_port', 'target_user', 'target_password_env'].forEach(function (name) {
    var el = form.elements[name];
    if (el) {
      el.addEventListener('input', resetTestStatus);
      el.addEventListener('change', resetTestStatus);
    }
  });

  // Submit restore form
  form.addEventListener('submit', function (ev) {
    ev.preventDefault();
    var intoDir = form.elements['restore_into'].value === 'dir';
    var targetName = intoDir ? (form.elements['target_dir'].value || '').trim() : (form.elements['target_database'].value || '').trim();
    var confirmVal = (form.elements['confirm'].value || '').trim();
    var status = document.getElementById('restore-message');

    if (confirmVal !== targetName) {
      if (status) { status.textContent = 'Type the target name exactly to confirm.'; status.classList.add('err'); }
      return;
    }

    var fromBackup = form.elements['restore_from'].value === 'backup';
    var connSel = (form.elements['connection_id'].value || '').trim();

    var payload = {
      confirm: confirmVal
    };

    if (fromBackup) {
      payload.backup_id = (form.elements['backup_id'].value || '').trim();
    } else {
      payload.source = (form.elements['source'].value || '').trim();
    }

    var tt = (form.elements['target_time'].value || '').trim();
    if (tt) {
      payload.target_time = normalizeTargetTime(tt);
    }

    if (connSel && connSel !== 'custom') {
      payload.connection_id = connSel;
    } else {
      payload.target = {
        host: (form.elements['target_host'].value || '').trim(),
        port: parseInt(form.elements['target_port'].value, 10) || 0,
        user: (form.elements['target_user'].value || '').trim(),
        password_env: (form.elements['target_password_env'].value || '').trim()
      };
    }

    if (intoDir) {
      payload.target_dir = targetName;
    } else {
      payload.target_database = targetName;
    }

    if (status) { status.textContent = 'Starting restore...'; status.classList.remove('err'); }

    window.strandApi('POST', '/api/v1/backup-restores', payload).then(function (res) {
      if (!res.ok) {
        return res.json().then(function (err) { throw new Error(err.error || 'Failed to start restore'); });
      }
      return res.json();
    }).then(function (data) {
      if (status) { status.textContent = 'Restore started successfully!'; }
      if (typeof window.strandToast === 'function') {
        window.strandToast('Restore job started', true);
      }
      setTimeout(function () {
        location.href = '/backups/jobs';
      }, 500);
    }).catch(function (err) {
      if (status) { status.textContent = err.message; status.classList.add('err'); }
    });
  });

  // Mark non-continuous sources in source select
  var srcSel = form.elements['source'];
  if (srcSel) {
    Array.prototype.forEach.call(srcSel.options, function (opt) {
      if (!opt.value) { return; }
      var sInfo = sources.find(function (x) { return x.source === opt.value; });
      if (sInfo && sInfo.continuous === false) {
        opt.disabled = true;
        if (!opt.textContent.includes('no continuous PITR')) {
          opt.textContent += ' (no continuous PITR)';
        }
      }
    });
  }

  // Initial synchronization
  syncRestoreFrom();
  syncRestoreInto();
  syncConnection();
  syncPitrWindow();
  filterConnectionsByEngine();
  updateStepper(1);

  // Global handle for test harnesses
  window.restoreWizard = {
    setStep: updateStepper,
    validateStep1: validateStep1,
    validateStep2: validateStep2,
    syncPitrWindow: syncPitrWindow,
    updateConfirm: updateConfirm
  };
})();
