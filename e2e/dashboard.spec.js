const { test, expect } = require('@playwright/test');

test('login, inspect portfolio, and persist shared manual crypto edits', async ({ page }) => {
  await page.goto('/');
  await page.locator('#login-username').fill('wrong');
  await page.locator('#login-password').fill('wrong');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.locator('#login-feedback')).toContainText('Invalid credentials');

  await page.locator('#login-username').fill('demo');
  await page.locator('#login-password').fill('test-password');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.locator('#total-value')).toContainText('30,000');
  await expect(page.locator('#rows-body')).toContainText('BTC');
  await expect(page.locator('#positions-count')).toContainText('4 assets · 6 snapshot rows');
  await expect(page.locator('#ok-positions')).toContainText('4 of 6 rows status ok');
  await expect(page.locator('#rows-body')).toContainText('incomplete');
  await expect(page.locator('#rows-body')).toContainText('UNPRICED');
  await expect(page.locator('#rows-body')).toContainText('Unavailable');
  await expect(page.locator('#type-allocations')).toContainText('crypto');
  await expect(page.locator('#source-allocations')).toContainText('Binance');
  await expect(page.locator('#rows-summary')).toContainText('Weights use the full portfolio total');
  if (await page.evaluate(() => window.innerWidth) < 500) {
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(await page.evaluate(() => window.innerWidth));
  }

  await page.locator('#holdings-search').fill('GGAL');
  await expect(page.locator('#rows-body')).toContainText('GGAL');
  await expect(page.locator('#rows-body')).not.toContainText('BTC');
  await page.getByRole('button', { name: 'Reset', exact: true }).click();
  await page.locator('#source-filter').selectOption({ label: 'Binance' });
  await expect(page.locator('#rows-body')).toContainText('BTC');
  await expect(page.locator('#rows-body')).toContainText('26,000');
  await expect(page.locator('#rows-body')).toContainText('86.67%');
  await page.getByRole('button', { name: 'Reset', exact: true }).click();
  if (await page.evaluate(() => window.innerWidth) < 500) {
    await page.locator('#sort-select').selectOption('name:asc');
  } else {
    await page.getByRole('button', { name: /Asset/ }).click();
    await page.getByRole('button', { name: /Asset/ }).click();
  }
  await expect(page.locator('#rows-body tr').first()).toContainText('BTC');
  if (await page.evaluate(() => window.innerWidth) < 500) {
    await page.locator('#sort-select').selectOption('value:desc');
  } else {
    await page.getByRole('button', { name: /Asset/ }).click();
  }
  await page.getByRole('button', { name: /Show source details for BTC/ }).click();
  await expect(page.locator('#rows-body')).toContainText('Exodus');
  await expect(page.locator('#rows-body')).toContainText('0.225');

  await page.getByRole('button', { name: 'Manage holdings' }).click();
  await expect(page.locator('#manual-modal')).toBeVisible();
  const symbolInputs = page.locator('#crypto-body input[data-field="symbol"]');
  const existingSymbols = await symbolInputs.evaluateAll((inputs) => inputs.map((input) => input.value));
  for (let index = existingSymbols.length - 1; index >= 0; index -= 1) {
    if (existingSymbols[index] === 'ETH') {
      await page.locator('#crypto-body tr').nth(index).getByRole('button', { name: /Remove row/ }).click();
    }
  }
  await page.getByRole('button', { name: 'Add row' }).click();
  const row = page.locator('#crypto-body tr').last();
  await row.locator('input[data-field="symbol"]').fill('eth');
  await row.locator('input[data-field="quantity"]').fill('1.5');
  await page.getByRole('button', { name: 'Save holdings' }).click();
  await expect(page.locator('#crypto-feedback')).toContainText('next valuation run');
  await page.getByRole('button', { name: 'Close manual holdings' }).click();
  await page.reload();
  await page.getByRole('button', { name: 'Manage holdings' }).click();
  await expect.poll(() => page.locator('#crypto-body input[data-field="symbol"]').evaluateAll((inputs) => inputs.map((input) => input.value))).toEqual(['XRP', 'ETH']);
  const preservedAccountId = await page.evaluate(async () => {
    const auth = JSON.parse(sessionStorage.getItem('fintracker.auth'));
    const response = await fetch('/api/manual/crypto-holdings', { headers: { Authorization: `Bearer ${auth.token}` } });
    const payload = await response.json();
    return payload.holdings.find((holding) => holding.symbol === 'XRP')?.account_id;
  });
  expect(preservedAccountId).toBe('other-account');
});

test('restored session loads saved account and account switching clears prior rows', async ({ page }) => {
  await page.goto('/');
  await page.locator('#login-username').fill('demo');
  await page.locator('#login-password').fill('test-password');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.locator('#rows-body')).toContainText('BTC');
  await page.reload();
  await expect(page.locator('#total-value')).toContainText('30,000');
  await expect(page.locator('#rows-body')).toContainText('BTC');
  await page.route(/\/api\/valuations\/latest\?account_id=c61899e0$/, (route) => route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'Fixture refresh failure.' }) }));
  await page.getByRole('button', { name: 'Refresh snapshot' }).click();
  await expect(page.locator('#feedback')).toContainText('Showing the snapshot from');
  await expect(page.locator('#total-value')).toContainText('30,000');
  await page.unroute(/\/api\/valuations\/latest\?account_id=c61899e0$/);
  await page.getByRole('button', { name: 'Edit account' }).click();
  await page.locator('#account-id-dialog').fill('other-account');
  await page.getByRole('button', { name: 'Load account' }).click();
  await expect(page.locator('#total-value')).toContainText('5,000');
  await expect(page.locator('#rows-body')).toContainText('ALT');
  await expect(page.locator('#rows-body')).not.toContainText('BTC');
  await page.getByRole('button', { name: 'Edit account' }).click();
  await page.locator('#account-id-dialog').fill('missing-account');
  await page.getByRole('button', { name: 'Load account' }).click();
  await expect(page.locator('#rows-body')).toContainText('No snapshot is available');
  await expect(page.locator('#rows-body')).not.toContainText('ALT');
});

test('late manual 401 after sign out does not expire a newer session state', async ({ page }) => {
  await page.goto('/');
  await page.locator('#login-username').fill('demo');
  await page.locator('#login-password').fill('test-password');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.locator('#rows-body')).toContainText('BTC');
  await page.route('**/api/manual/crypto-holdings', async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 250));
    await route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ detail: 'Obsolete token.' }) }).catch(() => {});
  });
  await page.getByRole('button', { name: 'Manage holdings' }).click();
  await page.locator('#sign-out-btn').evaluate((button) => button.click());
  await expect(page.locator('#workspace')).toBeHidden();
  await page.waitForTimeout(350);
  await expect(page.locator('#login-feedback')).toHaveText('Signed out.');
});

test('expired bearer token clears the session and asks for login', async ({ page }) => {
  await page.goto('/');
  await page.evaluate(() => sessionStorage.setItem('fintracker.auth', JSON.stringify({ token: 'expired-token', expiresAt: Date.now() + 60_000 })));
  await page.route('**/api/manual/crypto-holdings', (route) => route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ detail: 'Token expired.' }) }));
  await page.reload();
  await expect(page.locator('#login-feedback')).toContainText('Session expired');
  await expect.poll(() => page.evaluate(() => sessionStorage.getItem('fintracker.auth'))).toBeNull();
});

test('job health displays recorded history and reports an active run conflict', async ({ page }) => {
  await page.goto('/jobs.html');
  await page.locator('#username').fill('demo');
  await page.locator('#password').fill('test-password');
  await page.getByRole('button', { name: 'Log in & load' }).click();
  await expect(page.locator('#latest-status')).toHaveText('Success');
  await expect(page.locator('table')).toContainText('2026-09-30_120000');

  await page.getByRole('button', { name: 'Run now' }).click();
  await expect(page.locator('#feedback')).toContainText('already running');
});
