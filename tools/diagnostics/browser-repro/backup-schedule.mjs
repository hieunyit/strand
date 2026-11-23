// STRAND_DUMP_UI_DIR=<directory> go test ./internal/web -run '^TestDumpUIPagesForHarness$'
// NODE_PATH=<directory containing playwright> CHROMIUM_PATH=<optional executable> node tools/diagnostics/browser-repro/backup-schedule.mjs <directory>
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const { chromium } = createRequire(import.meta.url)('playwright');
const staticDir = fileURLToPath(new URL('../../../internal/web/static/', import.meta.url));
const fixtureDir = process.argv[2];
assert(fixtureDir, 'Pass the directory containing the rendered wizard fixtures.');

const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_PATH,
  args: ['--no-sandbox'],
});
try {
  const page = await browser.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('http://strand.test/**', async route => {
    const { pathname } = new URL(route.request().url());
    if (/^\/api\/v1\/backup-connections\/[^/]+\/test$/.test(pathname)) {
      return route.fulfill({ json: { ok: true, version: '8.4', databases: ['app'] } });
    }
    if (pathname === '/api/v1/backup-schedules/preview') {
      return route.fulfill({ json: { runs: [] } });
    }
    if (route.request().method() === 'PUT' && /^\/api\/v1\/backup-jobs\/[^/]+$/.test(pathname)) {
      return route.fulfill({ json: { job: { id: pathname.split('/').pop() }, warnings: ['Fixture saved'] } });
    }
    if (pathname.startsWith('/static/')) {
      return route.fulfill({ path: path.join(staticDir, pathname.slice('/static/'.length)) });
    }
    if (pathname === '/wizard_edit.html' || pathname === '/wizard_schedule_edit.html') {
      return route.fulfill({ contentType: 'text/html', body: await readFile(path.join(fixtureDir, pathname)) });
    }
    return route.fulfill({ status: 404 });
  });

  const payload = () => page.evaluate(() => window.strandJobWizard.buildPayload());
  const assertFollowsPolicy = async () => {
    const job = await payload();
    for (const field of ['schedule', 'window_minutes', 'jitter_seconds', 'retention']) {
      assert(!Object.hasOwn(job, field), `Inherited ${field} should be omitted`);
    }
  };
  const open = async fixture => {
    await page.goto('http://strand.test/' + fixture);
    await page.waitForFunction(() => document.querySelector('#wiz-test-status').textContent.includes('Connection works'));
    await page.locator('[data-goto="2"]').click();
  };

  await open('wizard_edit.html');
  await assertFollowsPolicy();
  assert.equal((await payload()).encryption_mode, 'inherit');

  await open('wizard_schedule_edit.html');
  const customize = page.locator('[name=customize]');
  assert(await customize.isChecked());
  await page.locator('#wiz-sched-block > details > summary').click();
  assert.equal(await page.locator('[name=ret_daily]').inputValue(), '12');
  assert.equal(await page.locator('[name=ret_maxage]').inputValue(), '90');
  const original = await payload();
  assert.deepEqual(original.schedule, { kind: 'interval', every_seconds: 91 });
  assert.equal(original.window_minutes, 19);
  assert.equal(original.jitter_seconds, 11);
  assert.deepEqual(original.retention, { keep_last: 4, keep_daily: 12, max_age_days: 90 });
  assert.equal(original.encryption_mode, 'on');

  await page.locator('[data-goto="1"]').click();
  await page.locator('#job-wizard-form [name=name]').fill('unrelated rename');
  await page.locator('[data-goto="2"]').click();
  const renamed = await payload();
  assert.deepEqual(renamed, { ...original, name: 'unrelated rename' });

  await page.locator('[data-goto="4"]').click();
  const saved = page.waitForRequest(req => req.method() === 'PUT' && req.url().includes('/api/v1/backup-jobs/'));
  await page.locator('#wiz-create-only').click();
  assert.deepEqual((await saved).postDataJSON(), renamed);
  await page.locator('#wiz-done').waitFor();

  await open('wizard_schedule_edit.html');
  await page.locator('[name=sch_every_min]').fill('2');
  await page.locator('#wiz-sched-block > details > summary').click();
  await page.locator('[name=ret_daily]').fill('14');
  const rescheduled = await payload();
  assert.equal(rescheduled.schedule.every_seconds, 120);
  assert.equal(rescheduled.retention.keep_daily, 14);
  assert.equal(rescheduled.encryption_mode, 'on');
  await customize.uncheck();
  await assertFollowsPolicy();
  await customize.check();
  const reset = await payload();
  assert.equal(reset.schedule.kind, 'daily');
  assert.equal(reset.encryption_mode, 'on');
  assert.notDeepEqual(reset.retention, original.retention);
  assert.deepEqual(errors, []);
  console.log('Schedule and retention intent passed in Chromium.');
} finally {
  await browser.close();
}
