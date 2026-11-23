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
  var restoreEndpoint = "";
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
    if (s.indexOf('.') !== -1) {
      s = s.split('.')[0];
    }
    if (s.length === 16) {
      s += ':00';
    }
    return s + '+07:00';
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

  function filterConnectionsByEngine() {
    var eng = getChosenEngine();
    var deployment = document.getElementById('choice-restore-deployment');
    if (deployment) {
      deployment.hidden = eng !== 'mongodb';
      if (deployment.hidden && form.elements['restore_into'].value === 'deployment') {
        form.elements['restore_into'].value = 'database';
        syncRestoreInto();
      }
    }
    var sel = document.getElementById('restore-conn-select');
    if (!sel) { return; }
    var hadSelected = sel.value;
    var selectedStillValid = false;
    Array.prototype.forEach.call(sel.options, function (opt) {
      if (!opt.value) {
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
    if (step === 4) {
      document.getElementById('restore-confirm-back').setAttribute('data-prev-step', curStep === 3 ? '3' : '2');
    }
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

    if (curStep === 3) {
      syncRestoreOptions();
    }

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
    syncRestoreOptions();
  }

  // Radio switching for restore_into
  function syncRestoreInto() {
    var intoDb = form.elements['restore_into'].value === 'database';
    var fDb = document.getElementById('field-target-db');
    var fDir = document.getElementById('field-target-dir');
    if (fDb) { fDb.hidden = !intoDb; }
    if (fDir) { fDir.hidden = form.elements['restore_into'].value !== 'dir'; }
    syncRestoreOptions();
  }

  // Target connection change
  function syncConnection() {
    restoreEndpoint = "";
    testOK = false;
    existingDBs = [];
    var testResult = document.getElementById('restore-test-result');
    if (testResult) { testResult.hidden = true; testResult.textContent = ''; }
    var status = document.getElementById('restore-test-status');
    if (status) { status.textContent = ''; }

    // If backup tool was pg_basebackup, enforce restore_into = dir
    var chosenTool = getChosenTool();
    var physical = document.getElementById('restore-physical-options');
    var choiceDb = document.getElementById('choice-restore-db');
    var radDir = form.querySelector('input[name="restore_into"][value="dir"]');

    if (chosenTool === 'pg_basebackup') {
      if (radDir) { radDir.checked = true; }
      if (physical) { physical.open = true; }
      if (choiceDb) { choiceDb.hidden = true; }
    } else {
      if (choiceDb) { choiceDb.hidden = false; }
    }
    syncRestoreInto();
    syncRestoreOptions();
  }

  function getChosenTool() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    if (fromBackup) {
      var bid = (form.elements['backup_id'].value || '').trim();
      var b = backups.find(function (x) { return x.id === bid; });
      return b ? (b.tool || '') : '';
    }
    return '';
  }

  function getChosenBackup() {
    var bid = (form.elements['backup_id'].value || '').trim();
    return backups.find(function (x) { return x.id === bid; });
  }

  function getChosenMethod() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    if (fromBackup) {
      var b = getChosenBackup();
      return b ? (b.method || b.tool || '') : '';
    }
    var src = (form.elements['source'].value || '').trim();
    var sInfo = sources.find(function (x) { return x.source === src; });
    if (sInfo && sInfo.base_backups && sInfo.base_backups.length > 0) {
      var b = sInfo.base_backups[sInfo.base_backups.length - 1];
      if (b && (b.method || b.tool)) {
        return b.method || b.tool;
      }
    }
    var eng = sInfo ? (sInfo.engine || '') : '';
    if (eng === 'postgres') { return 'pg_dump'; }
    if (eng === 'mysql' || eng === 'mariadb') { return 'mysqldump'; }
    if (eng === 'mongodb') { return 'mongodump'; }
    return '';
  }

  function getKnownBackupObjects() {
    var fromBackup = form.elements['restore_from'].value === 'backup';
    var b;
    if (fromBackup) {
      b = getChosenBackup();
    } else {
      var src = (form.elements['source'].value || '').trim();
      var sInfo = sources.find(function (x) { return x.source === src; });
      if (sInfo && sInfo.base_backups && sInfo.base_backups.length > 0) {
        b = sInfo.base_backups[sInfo.base_backups.length - 1];
      }
    }
    if (b) {
      if (b.details && b.details.objects_truncated === '1') {
        return null;
      }
      if (b.details && b.details.objects) {
        return b.details.objects.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
      }
      if (b.id && !b._fetchingObjects && window.strandApi) {
        b._fetchingObjects = true;
        window.strandApi('GET', '/api/v1/backups/' + encodeURIComponent(b.id)).then(function (res) {
          if (res.ok) { return res.json(); }
        }).then(function (data) {
          if (data && data.details) {
            b.details = b.details || {};
            if (data.details.objects) { b.details.objects = data.details.objects; }
            if (data.details.objects_truncated) { b.details.objects_truncated = data.details.objects_truncated; }
            syncRestoreOptions();
          }
        }).catch(function () {});
      }
    }
    return null;
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

    if (sInfo.continuous && sInfo.window_start && sInfo.window_end) {
      var wStart = window.strandLocalISO(sInfo.window_start);
      var wEnd = window.strandLocalISO(sInfo.window_end);
      if (timeInput) {
        timeInput.min = wStart;
        timeInput.max = wEnd;
      }
      if (winBox && winText) {
        winBox.hidden = false;
        winText.textContent = 'Recovery window for ' + src + ': ' +
          wStart.replace('T', ' ') + ' to ' +
          wEnd.replace('T', ' ') +
          ' UTC+7 (' + (sInfo.logs_count || 0) + ' log segments available).';
      }
    } else {
      if (winBox) { winBox.hidden = !sInfo.verify_on_replay; }
      if (winText && sInfo.verify_on_replay) { winText.textContent = 'PostgreSQL verifies the requested recovery time when it replays the archived WAL.'; }
      if (timeInput) { timeInput.removeAttribute('min'); timeInput.removeAttribute('max'); }
    }

    // PITR preview
    if (preview && timeInput && timeInput.value) {
      preview.textContent = 'Strand checks the available base backup and log chain before starting the restore.';
    } else if (preview) {
      preview.textContent = '';
    }
    syncRestoreOptions();
  }

  var renderedObjectsKey = '';

  function renderPicklist(objects) {
    var key = objects.join(',');
    if (renderedObjectsKey === key) { return; }
    renderedObjectsKey = key;
    var container = document.getElementById('restore-objects-picklist');
    if (!container) { return; }
    container.innerHTML = '';
    objects.forEach(function (obj, idx) {
      var label = document.createElement('label');
      label.className = 'pick check';
      var input = document.createElement('input');
      input.type = 'checkbox';
      input.name = 'restore_selected_object';
      input.value = obj;
      input.id = 'pick-obj-' + idx;
      var span = document.createElement('span');
      span.textContent = obj;
      label.appendChild(input);
      label.appendChild(span);
      container.appendChild(label);
    });
    updateObjectCount();
  }

  function updateObjectCount() {
    var countEl = document.getElementById('restore-objects-count');
    if (!countEl) { return; }
    var container = document.getElementById('restore-objects-picklist');
    if (!container) { return; }
    var checked = container.querySelectorAll('input[type=checkbox]:checked').length;
    countEl.textContent = checked + ' selected';
  }

  function getSelectedObjects() {
    var mode = (form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'].value) || 'all';
    if (mode !== 'partial') {
      return [];
    }
    var checkWrap = document.getElementById('restore-objects-checklist-wrap');
    if (checkWrap && !checkWrap.hidden) {
      var container = document.getElementById('restore-objects-picklist');
      if (!container) { return []; }
      var checked = container.querySelectorAll('input[type=checkbox]:checked');
      var list = [];
      checked.forEach(function (cb) { list.push(cb.value); });
      return list;
    }
    var textEl = document.getElementById('restore-objects-text');
    if (textEl) {
      var val = textEl.value.trim();
      if (!val) { return []; }
      return val.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
    }
    return [];
  }

  function syncRestoreOptions() {
    var method = getChosenMethod();
    var intoDir = form.elements['restore_into'] && form.elements['restore_into'].value === 'dir';
    var isPhysical = intoDir || method === 'pg_basebackup';

    var physNotice = document.getElementById('restore-opt-physical-notice');
    var mysqlNotice = document.getElementById('restore-opt-mysqldump-threads-notice');
    var mysqlPitrNotice = document.getElementById('restore-opt-mysql-pitr-notice');
    var threadsGrid = document.getElementById('restore-threads-grid');
    var objectsCards = document.getElementById('restore-objects-cards');
    var partialPanel = document.getElementById('restore-partial-panel');
    var choicePartial = document.getElementById('choice-objects-partial');
    var radAll = form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'][0];
    var radPartial = form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'][1];

    if (physNotice) { physNotice.hidden = !isPhysical; }

    if (isPhysical) {
      if (mysqlNotice) { mysqlNotice.hidden = true; }
      if (mysqlPitrNotice) { mysqlPitrNotice.hidden = true; }
      if (threadsGrid) { threadsGrid.hidden = true; }
      if (objectsCards) { objectsCards.hidden = true; }
      if (partialPanel) { partialPanel.hidden = true; }
      if (radAll) { radAll.checked = true; }
      return;
    }

    if (objectsCards) { objectsCards.hidden = false; }

    var isMySQL = (method === 'mysqldump' || method === 'mariadb-dump');
    var fromPITR = form.elements['restore_from'] && form.elements['restore_from'].value === 'pitr';
    var hasTargetTime = (form.elements['target_time'] && form.elements['target_time'].value.trim() !== '');
    var isMySQLPITR = isMySQL && (fromPITR || hasTargetTime);

    if (isMySQL) {
      if (mysqlNotice) { mysqlNotice.hidden = false; }
      if (threadsGrid) { threadsGrid.hidden = true; }
      var threadsInput = document.getElementById('restore-threads');
      if (threadsInput) { threadsInput.value = ''; }
    } else if (method === 'pg_dump' || method === 'mongodump') {
      if (mysqlNotice) { mysqlNotice.hidden = true; }
      if (threadsGrid) { threadsGrid.hidden = false; }
    } else {
      if (mysqlNotice) { mysqlNotice.hidden = true; }
      if (threadsGrid) { threadsGrid.hidden = true; }
    }

    if (isMySQLPITR) {
      if (mysqlPitrNotice) { mysqlPitrNotice.hidden = false; }
      if (choicePartial) { choicePartial.hidden = true; }
      if (radPartial && radPartial.checked) {
        if (radAll) { radAll.checked = true; }
      }
    } else {
      if (mysqlPitrNotice) { mysqlPitrNotice.hidden = true; }
      if (choicePartial) { choicePartial.hidden = false; }
    }

    var mode = (form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'].value) || 'all';
    if (mode === 'partial' && !isMySQLPITR) {
      if (partialPanel) { partialPanel.hidden = false; }
      var knownObjects = getKnownBackupObjects();
      var checkWrap = document.getElementById('restore-objects-checklist-wrap');
      var textWrap = document.getElementById('restore-objects-textarea-wrap');
      if (knownObjects && knownObjects.length > 0) {
        if (checkWrap) { checkWrap.hidden = false; }
        if (textWrap) { textWrap.hidden = true; }
        renderPicklist(knownObjects);
      } else {
        if (checkWrap) { checkWrap.hidden = true; }
        if (textWrap) { textWrap.hidden = false; }
      }
    } else {
      if (partialPanel) { partialPanel.hidden = true; }
    }
  }

  function validateStep3() {
    var advanced = document.getElementById('restore-advanced-options');
    var errThreads = document.getElementById('err-threads');
    var errObjects = document.getElementById('err-objects');
    if (errThreads) { errThreads.hidden = true; }
    if (errObjects) { errObjects.hidden = true; }

    var threadsGrid = document.getElementById('restore-threads-grid');
    if (threadsGrid && !threadsGrid.hidden) {
      var threadsInput = document.getElementById('restore-threads');
      if (threadsInput && threadsInput.value.trim() !== '') {
        var t = parseInt(threadsInput.value.trim(), 10);
        if (isNaN(t) || t < 1 || t > 16) {
          if (errThreads) {
            errThreads.textContent = 'Threads must be between 1 and 16.';
            errThreads.hidden = false;
          }
          if (advanced) { advanced.open = true; }
          return false;
        }
      }
    }

    var mode = (form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'].value) || 'all';
    if (mode === 'partial') {
      var objs = getSelectedObjects();
      if (objs.length === 0) {
        if (errObjects) {
          errObjects.textContent = 'Select or enter at least one object to restore, or choose "All objects".';
          errObjects.hidden = false;
        }
        if (advanced) { advanced.open = true; }
        return false;
      }
      if (objs.length > 1000) {
        if (errObjects) {
          errObjects.textContent = 'Too many objects: maximum is 1000.';
          errObjects.hidden = false;
        }
        if (advanced) { advanced.open = true; }
        return false;
      }
      var validRE = /^[A-Za-z0-9_$.-]+$/;
      for (var i = 0; i < objs.length; i++) {
        if (!validRE.test(objs[i])) {
          if (errObjects) {
            errObjects.textContent = 'Invalid object name "' + objs[i] + '": must match [A-Za-z0-9_$.-]';
            errObjects.hidden = false;
          }
          if (advanced) { advanced.open = true; }
          return false;
        }
      }
    }
    return true;
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
      } else if (!getChosenBackup() && backups.length < 1000) {
        // The page lists every complete restore point (up to 1000): anything else is failed, expired,
        // unfinished or unknown, and the server refuses it with 409.
        if (errBackup) {
          errBackup.textContent = (errBackup.getAttribute('data-msg-not-complete') || 'Not a complete restore point:') + ' ' + bid;
          errBackup.hidden = false;
        }
        valid = false;
      }
    } else {
      var src = (form.elements['source'].value || '').trim();
      if (!src) {
        if (errSource) { errSource.textContent = 'Select a source to restore from.'; errSource.hidden = false; }
        valid = false;
      } else {
        var sInfoContinuous = sources.find(function (x) { return x.source === src; });
        if (sInfoContinuous && sInfoContinuous.continuous === false && !sInfoContinuous.verify_on_replay) {
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
        if (sInfo && sInfo.continuous && sInfo.window_start && sInfo.window_end) {
          var tSel = new Date(normalizeTargetTime(tt));
          var tMin = new Date(sInfo.window_start);
          var tMax = new Date(sInfo.window_end);
          if (tSel < tMin || tSel > tMax) {
            if (errTime) {
              errTime.textContent = 'Target time is outside the recovery window [' +
                window.strandLocalISO(tMin).replace('T', ' ') + ', ' +
                window.strandLocalISO(tMax).replace('T', ' ') + '] (UTC+7).';
              errTime.hidden = false;
            }
            valid = false;
          }
        }
      }
    }
    if (!valid && !fromBackup) {
      document.getElementById('restore-pitr-options').open = true;
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
      if (errConn) { errConn.textContent = 'Choose the target connection.'; errConn.hidden = false; }
      return false;
    }

    if (!testOK) {
      if (errTest) {
        errTest.textContent = lastTestError || 'Test the connection successfully before continuing.';
        errTest.hidden = false;
      }
      return false;
    }

    var scope = form.elements['restore_into'].value;
    if (getChosenEngine() === 'mongodb' && !restoreEndpoint) {
      if (errTest) { errTest.textContent = 'Test a MongoDB connection with a valid endpoint.'; errTest.hidden = false; }
      return false;
    }
    if (scope === 'deployment' && existingDBs.some(function (name) {
      return name !== 'admin' && name !== 'config' && name !== 'local';
    })) {
      if (errTest) { errTest.textContent = 'Choose a MongoDB deployment with no application databases.'; errTest.hidden = false; }
      return false;
    }
    if (scope === 'database') {
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
    } else if (scope === 'dir') {
      var dirName = (form.elements['target_dir'].value || '').trim();
      if (!dirName) {
        document.getElementById('restore-physical-options').open = true;
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

      var p = window.strandApi('POST', '/api/v1/backup-connections/' + encodeURIComponent(connSel) + '/test');

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
        if ((form.elements['connection_id'].value || '').trim() !== connSel) { return; }
        var data = result.data;
        testOK = true;
        lastTestError = '';
        existingDBs = data.databases || [];
        restoreEndpoint = data.restore_endpoint || "";
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
    if (timeEl) { timeEl.textContent = tt ? tt.replace('T', ' ') + ' (UTC+7)' : 'Original backup snapshot'; }

    var connSel = (form.elements['connection_id'].value || '').trim();
    if (srvEl) {
      var c = connections.find(function (x) { return x.id === connSel; });
      srvEl.textContent = c ? c.name + ' (' + c.host + ':' + c.port + ')' : connSel;
    }

    var intoDir = form.elements['restore_into'].value === 'dir';
    if (destEl) {
      if (form.elements['restore_into'].value === 'deployment') {
        destEl.textContent = 'Empty MongoDB deployment';
      } else if (intoDir) {
        destEl.textContent = 'Directory: ' + (form.elements['target_dir'].value || '').trim();
      } else {
        destEl.textContent = 'Database: ' + (form.elements['target_database'].value || '').trim();
      }
    }

    var threadsInput = document.getElementById('restore-threads');
    var threadsGrid = document.getElementById('restore-threads-grid');
    var summaryThreads = document.getElementById('summary-threads');
    if (summaryThreads) {
      if (threadsGrid && !threadsGrid.hidden && threadsInput && threadsInput.value.trim() !== '') {
        summaryThreads.textContent = threadsInput.value.trim() + ' workers';
      } else {
        summaryThreads.textContent = 'Default';
      }
    }

    var summaryObjects = document.getElementById('summary-objects');
    if (summaryObjects) {
      var mode = (form.elements['restore_objects_mode'] && form.elements['restore_objects_mode'].value) || 'all';
      if (mode === 'all') {
        summaryObjects.textContent = 'All objects';
      } else {
        var objs = getSelectedObjects();
        var cnt = objs.length;
        var preview = objs.slice(0, 3).join(', ');
        if (cnt > 3) { preview += ', ...'; }
        summaryObjects.textContent = cnt + ' selected (' + preview + ')';
      }
    }
  }

  function confirmedDestination(targetName) {
    var scope = form.elements['restore_into'].value;
    if (getChosenEngine() !== 'mongodb') { return targetName; }
    if (scope === 'dir' || !restoreEndpoint) { return ''; }
    return scope === 'deployment' ? restoreEndpoint + '/*' : restoreEndpoint + '/' + targetName;
  }

  function updateConfirm() {
    var intoDir = form.elements['restore_into'].value === 'dir';
    var targetName = intoDir ? (form.elements['target_dir'].value || '').trim() : (form.elements['target_database'].value || '').trim();
    targetName = confirmedDestination(targetName);
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
      if (nextS === 4 && !validateStep3()) { updateStepper(3); return; }
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
        if (curStep <= 1 && targetS > 1 && !validateStep1()) { return; }
        if (curStep <= 2 && targetS > 2 && !validateStep2()) { return; }
        if (curStep <= 3 && targetS > 3 && !validateStep3()) { return; }
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
    if (ev.target.name === 'source' || ev.target.name === 'backup_id') { syncPitrWindow(); filterConnectionsByEngine(); syncRestoreOptions(); }
    if (ev.target.name === 'restore_objects_mode') { syncRestoreOptions(); }
  });

  form.addEventListener('input', function (ev) {
    if (ev.target.id === 'restore-confirm-input') { updateConfirm(); }
    if (ev.target.id === 'restore-target-database' || ev.target.id === 'restore-target-dir') { updateConfirm(); }
    if (ev.target.id === 'restore-target-time') { syncPitrWindow(); }
  });

  var filterInput = document.getElementById('restore-objects-filter');
  if (filterInput) {
    filterInput.addEventListener('input', function () {
      var q = filterInput.value.toLowerCase().trim();
      var container = document.getElementById('restore-objects-picklist');
      if (!container) { return; }
      var items = container.querySelectorAll('.pick');
      items.forEach(function (it) {
        var txt = (it.textContent || '').toLowerCase();
        it.hidden = q !== '' && txt.indexOf(q) === -1;
      });
    });
  }

  var selectAllBtn = document.getElementById('restore-objects-select-all');
  if (selectAllBtn) {
    selectAllBtn.addEventListener('click', function () {
      var container = document.getElementById('restore-objects-picklist');
      if (!container) { return; }
      var cbs = container.querySelectorAll('input[type=checkbox]');
      cbs.forEach(function (cb) {
        var parent = cb.closest('.pick');
        if (!parent || !parent.hidden) {
          cb.checked = true;
        }
      });
      updateObjectCount();
    });
  }

  var clearAllBtn = document.getElementById('restore-objects-clear-all');
  if (clearAllBtn) {
    clearAllBtn.addEventListener('click', function () {
      var container = document.getElementById('restore-objects-picklist');
      if (!container) { return; }
      var cbs = container.querySelectorAll('input[type=checkbox]');
      cbs.forEach(function (cb) { cb.checked = false; });
      updateObjectCount();
    });
  }

  var picklist = document.getElementById('restore-objects-picklist');
  if (picklist) {
    picklist.addEventListener('change', updateObjectCount);
  }

  // Submit restore form
  form.addEventListener('submit', function (ev) {
    ev.preventDefault();
    var intoDir = form.elements['restore_into'].value === 'dir';
    var targetName = intoDir ? (form.elements['target_dir'].value || '').trim() : (form.elements['target_database'].value || '').trim();
    var confirmVal = (form.elements['confirm'].value || '').trim();
    var status = document.getElementById('restore-message');

    if (confirmVal !== confirmedDestination(targetName)) {
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

    if (connSel) {
      payload.connection_id = connSel;
    }

    if (form.elements['restore_into'].value === 'deployment') {
      payload.target_deployment = true;
    } else if (intoDir) {
      payload.target_dir = targetName;
    } else {
      payload.target_database = targetName;
    }

    var threadsGrid = document.getElementById('restore-threads-grid');
    var threadsInput = document.getElementById('restore-threads');
    if (threadsGrid && !threadsGrid.hidden && threadsInput && threadsInput.value.trim() !== '') {
      var t = parseInt(threadsInput.value.trim(), 10);
      if (t > 0) {
        payload.threads = t;
      }
    }

    var objs = getSelectedObjects();
    if (objs.length > 0) {
      payload.objects = objs;
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
      if (sInfo && sInfo.continuous === false && !sInfo.verify_on_replay) {
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
  syncRestoreOptions();
  updateStepper(1);

  // Global handle for test harnesses
  window.restoreWizard = {
    setStep: updateStepper,
    validateStep1: validateStep1,
    validateStep2: validateStep2,
    validateStep3: validateStep3,
    syncRestoreOptions: syncRestoreOptions,
    syncPitrWindow: syncPitrWindow,
    updateConfirm: updateConfirm,
    getSelectedObjects: getSelectedObjects
  };
})();
