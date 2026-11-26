// Sign-in page: show/hide password, Caps Lock hint, matching new passwords, and a busy state on submit
// (no double submit). Kept apart from app.js: the login page loads no console code. CSP forbids inline JS.
(function () {
  'use strict';
  var form = document.querySelector('[data-login-form]');
  if (!form) { return; }

  Array.prototype.forEach.call(form.querySelectorAll('[data-reveal]'), function (btn) {
    var input = document.getElementById(btn.getAttribute('data-reveal'));
    if (!input) { return; }
    btn.addEventListener('click', function () {
      var show = input.type === 'password';
      input.type = show ? 'text' : 'password';
      btn.setAttribute('aria-pressed', show ? 'true' : 'false');
      btn.setAttribute('aria-label', btn.getAttribute(show ? 'data-label-hide' : 'data-label-show'));
      var use = btn.querySelector('use');
      if (use) { use.setAttribute('href', '/static/icons.svg#' + (show ? 'i-eye-off' : 'i-eye')); }
      input.focus();
    });
  });

  var caps = form.querySelector('.login-caps');
  function capsCheck(ev) {
    if (caps && ev.getModifierState) { caps.hidden = !ev.getModifierState('CapsLock'); }
  }
  Array.prototype.forEach.call(form.querySelectorAll('[data-caps]'), function (input) {
    input.addEventListener('keydown', capsCheck);
    input.addEventListener('keyup', capsCheck);
    input.addEventListener('blur', function () { if (caps) { caps.hidden = true; } });
  });

  var confirmInput = form.querySelector('[data-match]');
  function checkMatch() {
    if (!confirmInput) { return true; }
    var other = document.getElementById(confirmInput.getAttribute('data-match'));
    var ok = !other || other.value === confirmInput.value;
    confirmInput.setCustomValidity(ok ? '' : confirmInput.getAttribute('data-mismatch') || 'mismatch');
    return ok;
  }
  if (confirmInput) { confirmInput.addEventListener('input', checkMatch); }

  // Back/forward cache restores the page as it was when it was left: undo the busy state.
  window.addEventListener('pageshow', function () {
    var btn = form.querySelector('.login-submit');
    if (btn && btn.getAttribute('aria-busy') === 'true') { window.location.reload(); }
  });

  form.addEventListener('submit', function (ev) {
    checkMatch();
    if (!form.checkValidity()) {
      ev.preventDefault();
      form.reportValidity();
      return;
    }
    var btn = form.querySelector('.login-submit');
    if (btn) {
      if (btn.getAttribute('aria-busy') === 'true') { ev.preventDefault(); return; }
      btn.setAttribute('aria-busy', 'true');
      btn.classList.add('is-loading');
      var label = btn.querySelector('span');
      if (label && btn.getAttribute('data-busy-label')) { label.textContent = btn.getAttribute('data-busy-label'); }
    }
  });
})();
