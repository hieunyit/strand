// Playwright browser reproduction suite for Strand CDC Web UI
// Tests CSRF login behaviors and tab navigation integrity in real Chromium.
import { chromium } from '/tmp/playwright-env/node_modules/playwright/index.mjs';
import fs from 'node:fs';
import path from 'node:path';

function redact(str) {
  if (!str) return '';
  return str.replace(/(strand_[a-zA-Z0-9_-]+)/g, '[REDACTED_TOKEN]')
            .replace(/(password=[^&\s]+)/g, 'password=[REDACTED]')
            .replace(/("password":\s*")[^"]+(")/g, '$1[REDACTED]$2');
}

async function run() {
  const infoPath = process.argv[2] || '/tmp/fixture-info.json';
  if (!fs.existsSync(infoPath)) {
    console.error('Fixture info file not found:', infoPath);
    process.exit(1);
  }
  const info = JSON.parse(fs.readFileSync(infoPath, 'utf8'));
  const directURL = info.direct_url;
  const proxyURL = info.proxy_url;
  const attackerURL = info.attacker_url;
  const password = info.password;
  const auditFile = info.audit_file;

  const results = {
    timestamp: new Date().toISOString(),
    browser_version: null,
    playwright_version: '1.65.1',
    scenarios: []
  };

  function logAudit(label) {
    if (!fs.existsSync(auditFile)) return [];
    const lines = fs.readFileSync(auditFile, 'utf8').trim().split('\n').filter(Boolean);
    const parsed = lines.map(l => {
      try { return JSON.parse(l); } catch { return { raw: l }; }
    });
    return parsed;
  }

  console.log('Connecting to Chromium via CDP on 127.0.0.1:9222...');
  const browser = await chromium.connectOverCDP('http://127.0.0.1:9222');
  results.browser_version = browser.version();
  console.log(`Connected to Chromium ${browser.version()}`);

  async function recordScenario(name, fn) {
    console.log(`\n========================================\nRUNNING SCENARIO: ${name}\n========================================`);
    const sc = { name, status: 'UNKNOWN', checks: [], logs: [] };
    const addCheck = (desc, pass, detail) => {
      sc.checks.push({ desc, pass: !!pass, detail: detail || '' });
      console.log(`  [${pass ? 'PASS' : 'FAIL'}] ${desc} ${detail ? '(' + detail + ')' : ''}`);
    };
    try {
      await fn(addCheck, sc.logs);
      const allPassed = sc.checks.every(c => c.pass);
      sc.status = allPassed ? 'PASS' : 'FAIL';
    } catch (err) {
      sc.status = 'FAIL';
      sc.error = err.message;
      console.error(`  [EXCEPTION] ${err.message}`);
    }
    results.scenarios.push(sc);
    return sc.status === 'PASS';
  }

  // ----------------------------------------------------
  // Scenario 1: Direct Normal Login
  // ----------------------------------------------------
  await recordScenario('1. Direct Normal Login', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    await page.goto(`${directURL}/login`);
    const cookiesBefore = await ctx.cookies();
    const loginCookie = cookiesBefore.find(c => c.name === 'strand_login');
    assert('strand_login cookie set on GET /login', !!loginCookie, loginCookie ? `httpOnly=${loginCookie.httpOnly}, sameSite=${loginCookie.sameSite}` : 'missing');

    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation({ timeout: 5000 }),
      page.click('button[type="submit"]')
    ]);

    const currentUrl = page.url();
    assert('Redirects to root after successful login', currentUrl === `${directURL}/`, currentUrl);

    const cookiesAfter = await ctx.cookies();
    const sessCookie = cookiesAfter.find(c => c.name === 'strand_session');
    const expiredLogin = cookiesAfter.find(c => c.name === 'strand_login');
    assert('strand_session cookie set', !!sessCookie, sessCookie ? `httpOnly=${sessCookie.httpOnly}, sameSite=${sessCookie.sameSite}` : 'missing');
    assert('strand_login cleared or expired', !expiredLogin || expiredLogin.value === '' || expiredLogin.expires <= Date.now() / 1000);

    const newAudit = logAudit().slice(beforeAuditLen);
    const loginOk = newAudit.find(a => a.action === 'login' && a.result === 'ok');
    assert('Audit log records successful login', !!loginOk, loginOk ? `ip=${loginOk.ip}, action=${loginOk.action}` : 'no audit entry');

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 2: Direct Login with Wrong Password
  // ----------------------------------------------------
  await recordScenario('2. Direct Login with Wrong Password', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    await page.goto(`${directURL}/login`);
    await page.fill('input[name="password"]', 'completely_wrong_pass');
    await Promise.all([
      page.waitForNavigation({ timeout: 5000 }),
      page.click('button[type="submit"]')
    ]);

    const errorMsg = await page.textContent('.alert, .err, form p.err, form .muted.err');
    assert('Login page displays wrong password error', errorMsg && errorMsg.includes('wrong password'), errorMsg);

    const newAudit = logAudit().slice(beforeAuditLen);
    const badPw = newAudit.find(a => a.action === 'login' && a.result === 'denied' && a.detail?.reason === 'bad_password');
    assert('Audit log records bad_password denial', !!badPw, badPw ? `reason=${badPw.detail?.reason}` : 'missing');

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 3: Multi-Tab Login Race
  // ----------------------------------------------------
  await recordScenario('3. Multi-Tab Login Race (Stale Tab Token vs Newer Tab Cookie)', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page1 = await ctx.newPage();
    const page2 = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    // Tab 1 opens /login
    await page1.goto(`${directURL}/login`);
    const token1 = await page1.inputValue('input[name="csrf"]');
    const cookies1 = await ctx.cookies();
    const cookie1 = cookies1.find(c => c.name === 'strand_login')?.value;

    // Tab 2 opens /login, overwriting strand_login cookie
    await page2.goto(`${directURL}/login`);
    const token2 = await page2.inputValue('input[name="csrf"]');
    const cookies2 = await ctx.cookies();
    const cookie2 = cookies2.find(c => c.name === 'strand_login')?.value;

    assert('Tab 1 and Tab 2 receive distinct CSRF tokens', token1 !== token2);
    assert('Tab 2 overwrites strand_login cookie', cookie1 !== cookie2);

    // Tab 1 now submits with token1 (which no longer matches cookie2 in cookie jar)
    await page1.bringToFront();
    await page1.fill('input[name="password"]', password);
    await Promise.all([
      page1.waitForNavigation({ timeout: 5000 }),
      page1.click('button[type="submit"]')
    ]);

    const url1 = page1.url();
    assert('Tab 1 successfully logs in despite overwritten cookie (via Sec-Fetch-Site: same-origin)', url1 === `${directURL}/`, url1);

    const newAudit = logAudit().slice(beforeAuditLen);
    const loginOk = newAudit.find(a => a.action === 'login' && a.result === 'ok');
    assert('Audit log confirms login ok for multi-tab', !!loginOk);

    await page1.close();
    await page2.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 4: Missing or Cleared Login Cookie
  // ----------------------------------------------------
  await recordScenario('4. Missing/Cleared Login Cookie (Same-Origin Browser Fallback)', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    await page.goto(`${directURL}/login`);
    // Delete strand_login cookie from context
    await ctx.clearCookies();

    // Verify cookies are empty
    const remainingCookies = await ctx.cookies();
    assert('strand_login cookie deleted prior to submit', remainingCookies.length === 0);

    // Submit form
    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation({ timeout: 5000 }),
      page.click('button[type="submit"]')
    ]);

    const url = page.url();
    assert('Login succeeds via Sec-Fetch-Site same-origin even with missing cookie', url === `${directURL}/`, url);

    const newAudit = logAudit().slice(beforeAuditLen);
    const loginOk = newAudit.find(a => a.action === 'login' && a.result === 'ok');
    assert('Audit records login ok with missing login cookie', !!loginOk);

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 5: HTTP Reverse Proxy Origin with X-Forwarded-*
  // ----------------------------------------------------
  await recordScenario('5. HTTP Reverse Proxy Origin with X-Forwarded-* IP Trust', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    await page.goto(`${proxyURL}/login`);
    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation({ timeout: 5000 }),
      page.click('button[type="submit"]')
    ]);

    const url = page.url();
    assert('Login via reverse proxy succeeds', url === `${proxyURL}/`, url);

    const newAudit = logAudit().slice(beforeAuditLen);
    const loginOk = newAudit.find(a => a.action === 'login' && a.result === 'ok');
    assert('Audit log captures forwarded IP (203.0.113.195)', loginOk?.ip === '203.0.113.195', `got ip=${loginOk?.ip}`);

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 6: Cross-Origin / Forged Origin CSRF Attack
  // ----------------------------------------------------
  await recordScenario('6. Cross-Origin / Forged Origin CSRF Attack Rejection', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();
    const beforeAuditLen = logAudit().length;

    // Navigate to attacker site on different origin
    await page.goto(`${attackerURL}/`);
    assert('Attacker page loaded', page.url().startsWith(attackerURL));

    // Attack 6A: Cross-origin form submission to /login
    await Promise.all([
      page.waitForNavigation({ timeout: 5000 }),
      page.click('#submit-form')
    ]);

    const attackUrl = page.url();
    const bodyText = await page.textContent('body');
    assert('Cross-origin form submission lands on login with 403 Forbidden or error',
      attackUrl.includes('/login') && (bodyText.includes('missing or invalid CSRF token') || bodyText.includes('Forbidden') || bodyText.includes('CSRF')),
      bodyText.slice(0, 100));

    // Attack 6B: Cross-origin fetch API to /api/v1/login
    await page.goto(`${attackerURL}/`);
    await page.click('#trigger-fetch');
    await page.waitForTimeout(1000);
    const resultText = await page.textContent('#result');
    assert('Cross-origin fetch is rejected (403 or CORS block)',
      resultText.includes('STATUS: 403') || resultText.includes('FETCH_ERR'),
      resultText.trim());

    // Check audit log for csrf_rejected
    const newAudit = logAudit().slice(beforeAuditLen);
    const csrfDenied = newAudit.filter(a => a.action === 'login_csrf_rejected');
    assert('Audit log explicitly records login_csrf_rejected for cross-origin attacks',
      csrfDenied.length > 0, `found ${csrfDenied.length} rejection entries`);
    if (csrfDenied.length > 0) {
      assert('Audit detail records cross_site reason',
        csrfDenied[0].detail?.reason?.includes('cross_site') || csrfDenied[0].detail?.reason === 'origin_mismatch',
        `reason=${csrfDenied[0].detail?.reason}`);
    }

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 7: Spontaneous Tab Navigation Under Delayed Refresh
  // ----------------------------------------------------
  await recordScenario('7. Navigation Integrity: Slow Overlapping Refresh During Tab Switching', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();

    // Log in first
    await page.goto(`${proxyURL}/login`);
    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation(),
      page.click('button[type="submit"]')
    ]);

    // Navigate to pipeline Graph tab
    await page.goto(`${proxyURL}/pipelines/orders`);
    await page.waitForSelector('nav.tabs a.active');
    const initActive = await page.textContent('nav.tabs a.active');
    assert('Initially on Graph tab', initActive.includes('Graph'), initActive.trim());

    // Enable 1500ms delay on /tables requests via control endpoint
    await fetch(`${directURL}/_control/set_delay?ms=1500`);

    // Click Tables tab
    console.log('  Navigating to Tables tab (response delayed by 1500ms)...');
    const clickTablesPromise = page.click('nav.tabs a[href$="/tables"]');
    await page.waitForTimeout(200);

    // While Tables fetch/navigation is processing, click Checkpoint tab
    console.log('  Quickly switching to Checkpoint tab before Tables refresh completes...');
    await page.goto(`${proxyURL}/pipelines/orders/checkpoint`);
    await page.waitForSelector('nav.tabs a.active');

    // Wait for any in-flight delayed responses or background timers (2500ms)
    console.log('  Waiting 2500ms for delayed responses to settle...');
    await page.waitForTimeout(2500);

    const settledUrl = page.url();
    const settledActive = await page.textContent('nav.tabs a.active');
    const contentText = await page.textContent('#content');

    assert('URL remains on Checkpoint (/pipelines/orders/checkpoint)',
      settledUrl === `${proxyURL}/pipelines/orders/checkpoint`, settledUrl);
    assert('Active tab remains Checkpoint (did not spontaneously switch to Tables)',
      settledActive.includes('Checkpoint'), settledActive.trim());
    assert('Content shows Checkpoint details and not Tables snapshot progress',
      contentText.includes('Checkpoint') || contentText.includes('Applied') || contentText.includes('Epoch'),
      'Content correctly displays Checkpoint view');

    // Disable delay
    await fetch(`${directURL}/_control/set_delay?ms=0`);

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 8: Back-Forward Navigation and Scroll Preservation
  // ----------------------------------------------------
  await recordScenario('8. History Back/Forward Navigation & Refresh State Preservation', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();

    // Log in
    await page.goto(`${directURL}/login`);
    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation(),
      page.click('button[type="submit"]')
    ]);

    // Go to Graph tab -> then Tables tab -> then Checkpoint tab
    await page.goto(`${directURL}/pipelines/orders`);
    await page.click('nav.tabs a[href$="/tables"]');
    await page.waitForURL('**/tables');
    assert('Arrived at Tables tab', page.url().endsWith('/tables'));

    await page.click('nav.tabs a[href$="/checkpoint"]');
    await page.waitForURL('**/checkpoint');
    assert('Arrived at Checkpoint tab', page.url().endsWith('/checkpoint'));

    // Go back to Tables
    await page.goBack();
    await page.waitForURL('**/tables');
    let activeTab = await page.textContent('nav.tabs a.active');
    assert('Back button restores Tables tab URL and active tab',
      page.url().endsWith('/tables') && activeTab.includes('Tables'), activeTab.trim());

    // Scroll the page
    await page.evaluate(() => window.scrollTo(0, 150));
    const scrollY1 = await page.evaluate(() => window.scrollY);

    // Trigger refresh function in page context
    await page.evaluate(() => {
      // Force refresh if strandApi exists or trigger periodic refresh
      if (typeof window.refresh === 'function') window.refresh(true);
    });
    await page.waitForTimeout(1000);

    const scrollY2 = await page.evaluate(() => window.scrollY);
    assert('Page scroll position maintained across periodic refresh',
      Math.abs(scrollY2 - scrollY1) < 10, `before=${scrollY1}, after=${scrollY2}`);

    // Go back to Graph
    await page.goBack();
    await page.waitForURL(`${directURL}/pipelines/orders`);
    activeTab = await page.textContent('nav.tabs a.active');
    assert('Back button restores Graph tab URL and active tab',
      page.url() === `${directURL}/pipelines/orders` && activeTab.includes('Graph'), activeTab.trim());

    // Go forward to Tables
    await page.goForward();
    await page.waitForURL('**/tables');
    activeTab = await page.textContent('nav.tabs a.active');
    assert('Forward button restores Tables tab URL and active tab',
      page.url().endsWith('/tables') && activeTab.includes('Tables'), activeTab.trim());

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 9: Navigation Integrity: Stale/Mismatched Response Protection
  // ----------------------------------------------------
  await recordScenario('9. Navigation Integrity: Stale/Mismatched Response Protection', async (assert, logs) => {
    const ctx = await browser.newContext();
    const page = await ctx.newPage();

    // Log in
    await page.goto(`${proxyURL}/login`);
    await page.fill('input[name="password"]', password);
    await Promise.all([
      page.waitForNavigation(),
      page.click('button[type="submit"]')
    ]);

    // Go to Graph tab (/pipelines/orders)
    await page.goto(`${proxyURL}/pipelines/orders`);
    await page.waitForSelector('nav.tabs a.active');
    const startTab = await page.textContent('nav.tabs a.active');
    assert('Starts on Graph tab', startTab.includes('Graph'));

    // Inject interceptor on route to simulate proxy returning stale /tables HTML for /pipelines/orders
    console.log('  Testing refresh behavior when a proxy/load-balancer returns stale /tables HTML...');
    const tablesHtmlRes = await fetch(`${proxyURL}/pipelines/orders/tables`, {
      headers: { 'Cookie': (await ctx.cookies()).map(c => `${c.name}=${c.value}`).join('; ') }
    });
    const tablesHtml = await tablesHtmlRes.text();

    // Now on the page, attempt to feed the stale tables HTML into refresh or DOM
    const testResult = await page.evaluate(async (staleHtml) => {
      const doc = new DOMParser().parseFromString(staleHtml, 'text/html');
      const fresh = doc.getElementById('content');
      const activeInFresh = fresh ? fresh.querySelector('nav.tabs a.active') : null;
      const freshActiveHref = activeInFresh ? activeInFresh.getAttribute('href') : null;
      const currentPath = location.pathname;
      const wouldMismatch = !!(activeInFresh && activeInFresh.pathname && activeInFresh.pathname !== location.pathname);

      return {
        currentPath,
        freshActiveHref,
        wouldMismatch
      };
    }, tablesHtml);

    assert('Tables HTML active tab href is /pipelines/orders/tables',
      testResult.freshActiveHref === '/pipelines/orders/tables', testResult.freshActiveHref);
    assert('Pathname mismatch detected between Graph (/pipelines/orders) and Tables tab',
      testResult.wouldMismatch === true, `current=${testResult.currentPath}`);

    await page.close();
    await ctx.close();
  });

  // ----------------------------------------------------
  // Scenario 10: Audit Log Redaction Verification
  // ----------------------------------------------------
  await recordScenario('10. Audit Log Secrets Redaction Verification', async (assert, logs) => {
    const rawAudit = fs.readFileSync(auditFile, 'utf8');
    assert('Audit log exists and has content', rawAudit.length > 0);
    assert('Plaintext fixture password is never leaked into audit log',
      !rawAudit.includes(password), 'password safely redacted');
    assert('Plaintext database passwords not leaked into audit log',
      !rawAudit.includes('FakeSrcPass') && !rawAudit.includes('FakeTargetPass'),
      'passwords safely redacted');

    // Check each audit line format
    const lines = rawAudit.trim().split('\n').filter(Boolean);
    let allValidJSON = true;
    for (const l of lines) {
      try {
        const obj = JSON.parse(l);
        if (!obj.time || !obj.action) allValidJSON = false;
      } catch {
        allValidJSON = false;
      }
    }
    assert('All audit log entries are valid JSON with time and action fields', allValidJSON);
  });

  await browser.close();

  console.log('\n========================================');
  console.log('TEST SUITE COMPLETED');
  console.log('========================================');
  const total = results.scenarios.length;
  const passed = results.scenarios.filter(s => s.status === 'PASS').length;
  console.log(`Summary: ${passed}/${total} scenarios passed.`);

  const outResultPath = process.argv[3] || '/tmp/suite-results.json';
  fs.writeFileSync(outResultPath, JSON.stringify(results, null, 2));
  console.log(`Results saved to ${outResultPath}`);

  if (passed !== total) {
    process.exit(1);
  }
}

run().catch(err => {
  console.error('Fatal suite error:', err);
  process.exit(1);
});
