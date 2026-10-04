const { chromium } = require('playwright');
const fs = require('fs/promises');
const path = require('path');

const DEFAULT_APP_URL = 'http://127.0.0.1:4173';
const SCREENSHOT_DIR = path.join('artifacts', 'visual-check');
const PAGE_LOAD_TIMEOUT_MS = 30_000;
const VIEWPORTS = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

async function main() {
  const appUrl = process.env.APP_URL || DEFAULT_APP_URL;
  await fs.mkdir(SCREENSHOT_DIR, { recursive: true });

  const browser = process.env.PLAYWRIGHT_WS_ENDPOINT
    ? await chromium.connect(process.env.PLAYWRIGHT_WS_ENDPOINT)
    : await chromium.launch();
  try {
    for (const viewport of VIEWPORTS) {
      const page = await browser.newPage({ viewport });
      await page.goto(appUrl, { waitUntil: 'networkidle', timeout: PAGE_LOAD_TIMEOUT_MS });
      const screenshotPath = path.join(SCREENSHOT_DIR, `${viewport.name}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`Saved ${viewport.name} screenshot: ${screenshotPath}`);
      if (await page.locator('#login-form').count()) {
        await page.locator('#login-username').fill('demo');
        await page.locator('#login-password').fill('test-password');
        await page.getByRole('button', { name: 'Sign in' }).click();
        await page.locator('#total-value').waitFor({ state: 'visible' });
        await page.locator('#rows-body').getByText('BTC', { exact: true }).waitFor();
        await page.screenshot({ path: path.join(SCREENSHOT_DIR, `${viewport.name}-authenticated.png`), fullPage: true });
        const details = page.getByRole('button', { name: /Show source details for BTC/ });
        if (await details.count()) {
          await details.first().click();
          await page.screenshot({ path: path.join(SCREENSHOT_DIR, `${viewport.name}-expanded.png`), fullPage: true });
        }
        await page.getByRole('button', { name: 'Manage holdings' }).click();
        await page.screenshot({ path: path.join(SCREENSHOT_DIR, `${viewport.name}-editor.png`), fullPage: true });
      }
      await page.close();
    }
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
