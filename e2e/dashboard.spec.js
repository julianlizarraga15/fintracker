const { test, expect } = require('@playwright/test');

test('login, load portfolio, and persist manual crypto edits', async ({ page }) => {
  await page.goto('/');
  await page.locator('#login-username').fill('wrong');
  await page.locator('#login-password').fill('wrong');
  await page.getByRole('button', { name: 'Log in' }).click();
  await expect(page.locator('#login-feedback')).toContainText('Invalid credentials');

  await page.locator('#login-username').fill('demo');
  await page.locator('#login-password').fill('test-password');
  await page.getByRole('button', { name: 'Log in' }).click();
  await expect(page.locator('#total-value')).toContainText('30,000');
  await expect(page.locator('#rows-body')).toContainText('BTC');

  await page.getByRole('button', { name: 'Add row' }).click();
  const row = page.locator('#crypto-body tr').last();
  await row.locator('input[data-field="symbol"]').fill('eth');
  await row.locator('input[data-field="quantity"]').fill('1.5');
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.locator('#crypto-feedback')).toContainText('saved');
  await page.reload();
  await page.getByRole('button', { name: 'Load', exact: true }).click();
  await expect(page.locator('#crypto-body')).toContainText('ETH');
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
