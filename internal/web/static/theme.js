// Applies the saved colour theme before first paint (loaded synchronously in <head>; no inline script because of the CSP).
(function () {
  try {
    var t = localStorage.getItem('strand-theme');
    if (t === 'light' || t === 'dark') { document.documentElement.setAttribute('data-theme', t); }
  } catch (e) { /* storage blocked: follow the system setting */ }
}());
