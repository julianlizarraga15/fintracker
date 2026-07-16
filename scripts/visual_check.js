const { chromium } = require('playwright');
const fs = require('fs/promises');
const path = require('path');

const DEFAULT_APP_URL = 'http://localhost:8080';
const SCREENSHOT_DIR = path.join('artifacts', 'visual-check');
const PAGE_LOAD_TIMEOUT_MS = 30_000;
const VIEWPORTS = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 },
];

async function main() {
  const appUrl = process.env.APP_URL || DEFAULT_APP_URL;
  await fs.mkdir(SCREENSHOT_DIR, { recursive: true });

  const browser = await chromium.launch();
  try {
    for (const viewport of VIEWPORTS) {
      const page = await browser.newPage({ viewport });
      await page.goto(appUrl, { waitUntil: 'networkidle', timeout: PAGE_LOAD_TIMEOUT_MS });
      const screenshotPath = path.join(SCREENSHOT_DIR, `${viewport.name}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`Saved ${viewport.name} screenshot: ${screenshotPath}`);
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
