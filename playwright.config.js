const { defineConfig } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list'], ['html', { outputFolder: 'artifacts/e2e/report', open: 'never' }]],
  outputDir: 'artifacts/e2e/results',
  use: {
    baseURL: process.env.E2E_BASE_URL || 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    connectOptions: process.env.PLAYWRIGHT_WS_ENDPOINT ? { wsEndpoint: process.env.PLAYWRIGHT_WS_ENDPOINT } : undefined,
  },
  projects: [
    { name: 'desktop', use: { browserName: 'chromium', viewport: { width: 1440, height: 900 } } },
    { name: 'mobile', use: { browserName: 'chromium', viewport: { width: 390, height: 844 } } },
  ],
  webServer: process.env.E2E_EXTERNAL_SERVER ? undefined : { command: 'node scripts/e2e_server.js', url: 'http://127.0.0.1:4173/health', reuseExistingServer: !process.env.CI, timeout: 60_000 },
});
