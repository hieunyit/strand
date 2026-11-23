import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const { chromium } = createRequire(import.meta.url)('playwright');
const fixtures = process.argv[2];
assert(fixtures, 'Pass the directory with the rendered PostgreSQL restore page.');
const staticDir = fileURLToPath(new URL('../../../internal/web/static/', import.meta.url));
const browser = await chromium.launch({executablePath: process.env.CHROMIUM_PATH, args: ['--no-sandbox', '--disable-dev-shm-usage']});
try {
  for (const width of [1280, 390]) {
    const page = await browser.newPage({viewport: {width, height: 900}});
    await page.route('http://strand.test/**', async route => {
      const pathname = new URL(route.request().url()).pathname;
      if (pathname.startsWith('/static/')) {
        await route.fulfill({path: path.join(staticDir, pathname.slice('/static/'.length))});
      } else if (pathname === '/restore') {
        await route.fulfill({contentType: 'text/html', body: await readFile(path.join(fixtures, 'restore_postgres_pitr.html'))});
      } else {
        await route.fulfill({status: 404});
      }
    });
    await page.goto('http://strand.test/restore');
    await page.locator('#restore-pitr-options summary').click();
    await page.getByText('Point in time from a source (PITR)', {exact: true}).click();
    const selection = page.locator('#restore-source-select');
    await selection.selectOption('pg-pitr');
    assert.equal(await selection.inputValue(), 'pg-pitr');
    const notice = page.locator('#restore-window-text');
    assert.match(await notice.textContent(), /verifies the requested recovery time/);
    const time = page.locator('#restore-target-time');
    assert.equal(await time.getAttribute('max'), null);
    await time.fill('2026-11-20T11:00:00');
    await page.locator('[data-goto="2"]').click();
    assert.equal(await page.locator('[data-step="2"]').isVisible(), true);
    await page.close();
  }
  console.log('PostgreSQL PITR replay: Chromium 1280px and 390px passed');
} finally {
  await browser.close();
}
