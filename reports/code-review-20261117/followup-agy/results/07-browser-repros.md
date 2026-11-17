# Task 07: Real Headless Browser Investigation — CSRF Login & Navigation Integrity

## Summary
Executed comprehensive browser investigation of user CSRF login and spontaneous Tables tab navigation using real headless Chromium (`Chromium 124.0.6367.78`) and Playwright (`v1.43.0` / suite runner `v1.65.1`).
The test suite executed inside an isolated environment via `tools/diagnostics/browser-repro/run.sh` against an isolated Go Web server fixture with full reverse-proxy origin simulation, session token lifecycle, and audit log generation.

In accordance with specifications and review rules:
1. **Real Browser Environment**: Ran against real headless Chromium over Chrome DevTools Protocol (CDP port 9222), without mocks or synthetic DOM emulations.
2. **Security & Least Privilege**: No real user accounts or credentials were used. Plaintext passwords, session cookies, and CSRF tokens are strictly redacted in telemetry logs, audit assertions, and reports (`[REDACTED]`).
3. **Dual Network Topologies Tested**: Validated direct browser connections and HTTP reverse-proxy setups (`X-Forwarded-For`, `X-Forwarded-Proto`, and trusted IP header propagation).
4. **Zero CSRF Weakening**: All cross-origin and forged-origin form submissions and fetches were rejected (`403 Forbidden`, `login_csrf_rejected`), proving that multi-tab and stale-token handling did not weaken CSRF protection.
5. **Navigation Evidence Limit**: The original scenario 9 only parsed HTML in test code and did not exercise application stale-response handling. It cannot support the original protection claim. A replacement application-path regression was run during the branch-review repair (details below).

---

## Owned Files Created / Modified
- `tools/diagnostics/browser-repro/server_fixture.go` (NEW): Isolated Go test fixture launching a Strand Web console server, an HTTP reverse proxy, and a cross-origin attacker server with session audit logging.
- `tools/diagnostics/browser-repro/suite.mjs` (NEW): Playwright browser test suite covering all 10 verification scenarios.
- `tools/diagnostics/browser-repro/run.sh` (NEW): Automated orchestrator managing isolated Chromium CDP container, server compilation, suite execution, and clean resource teardown.

---

## Browser Test Scenarios & Empirical Outcomes

| # | Scenario | Mechanism Tested | Outcome | Details |
|---|---|---|:---:|---|
| 1 | **Direct Normal Login** | `strand_login` cookie lifecycle, CSRF token exchange, redirect | `PASS` | `strand_login` cookie set (`HttpOnly=true, SameSite=Strict`), exchanged for `strand_session`, redirected to `/`. Audit log records `action=login`. |
| 2 | **Direct Wrong Password** | Rejection of bad credentials | `PASS` | Error displayed on login page. Audit log records `reason=bad_password`. |
| 3 | **Multi-Tab Login Race** | Tab 1 opened, Tab 2 opened (overwriting `strand_login` cookie), Tab 1 submitted | `PASS` | Tab 1 successfully authenticates via browser `Sec-Fetch-Site: same-origin` fallback without invalid token CSRF failure. |
| 4 | **Missing/Cleared Login Cookie** | User browser clears cookies prior to form submit | `PASS` | Login succeeds via `Sec-Fetch-Site: same-origin` check. Audit records `login ok`. |
| 5 | **Reverse Proxy & Forwarded Headers** | `X-Forwarded-For: 203.0.113.195`, `X-Forwarded-Proto: http` | `PASS` | Login succeeds through proxy. Audit log correctly captures client IP `203.0.113.195`. |
| 6 | **Cross-Origin / Forged Origin Attack** | Attacker domain `http://127.0.0.1:38017` POSTs to login | `PASS` | Cross-origin form POST returns `403 Forbidden`. Cross-origin fetch blocked. Audit log records `login_csrf_rejected` with `reason=cross_site_same-site`. |
| 7 | **Navigation Integrity: Overlapping Refresh** | Delayed `/tables` response (1500ms) while user switches to `/checkpoint` | `PASS` | Active tab remains Checkpoint. Content displays Checkpoint details; did not spontaneously revert to Tables. |
| 8 | **History Navigation & Scroll State** | Browser Back/Forward buttons and periodic polling refresh | `PASS` | Back/Forward accurately restores Tab states and URLs. Page scroll position (`150px`) preserved across periodic refreshes. |
| 9 (historical) | **Stale / Mismatched Response Protection** | Test-owned DOMParser mismatch calculation | `INSUFFICIENT` | Original PASS did not deliver a stale response to the application. Replaced by the repair regression below. |
| 10 | **Audit Log Secrets Redaction** | Inspection of audit trail for leaked credentials | `PASS` | Zero password or session secrets leaked in audit file; all entries valid JSON. |

---

## Lab Scenarios and Limits on User-Host Attribution

1. **User Login CSRF Failures in Real Browsers**:
   - **Reproduced mechanism**: When users open login in multiple tabs or have browser privacy extensions that purge short-lived cookies, `strand_login` cookie is overwritten by the latest tab. If the backend strictly checks `cookie(strand_login) == form(token)`, the earlier tab's form submission was rejected as CSRF forgery.
   - **Verification**: The Web server's modern fallback checking browser-enforced `Sec-Fetch-Site: same-origin` allows multi-tab submissions while continuing to strictly reject cross-origin attacks (Scenario 3, 4 vs 6).

2. **Spontaneous Navigation to Tables Tab**:
   - **Hypothesized mechanism**: If an asynchronous partial HTML refresh for `/tables` was in-flight when the user clicked another tab, the arriving late response could replace DOM elements without verifying `window.location.pathname`.
   - **Verification limit**: The original scenario 9 was insufficient. The replacement now routes a delayed HTTP response through the actual periodic `app.js` refresh and observes the rendered DOM. These fixture results do not establish the cause of a user-host incident.


## Branch-review repair and rerun (2026-11-17)

The Chromium wrapper now uses Docker's returned container ID for cleanup and an OS-allocated CDP port announced by that owned browser. It never stops the fixed `strand-chrome-fixture` name. The fixture's own temporary directory is removed on normal signal shutdown. A fake-Docker executable regression exercises the real wrapper and asserts that only the returned ID is stopped.

Scenario 9 now intercepts the application's `X-Requested-With: fetch` periodic request, holds it, and fulfills it with wrong-tab `/tables` HTML. A mutation observer and the active-tab/URL assertions verify that the application's refresh never installs the stale content. The application now checks the fresh response's active tab path before replacing content. A second actual in-flight response is released after a query-string history change and must also be discarded. Existing busy/sequence checks continue to serialize refresh generations.

The real owned Chromium 124.0.6367.78 fixture suite passed all 10 scenarios after the repair. Scenario 9 passed both actual-refresh assertions. Repair log: `/tmp/strand-repair-browser.log` (ephemeral local evidence). This is bounded fixture coverage, not proof that every user browser or proxy is unaffected.
