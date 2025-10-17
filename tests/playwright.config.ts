import path from 'node:path';
import { defineConfig, devices } from '@playwright/test';

const frontendUrl = process.env.FRONTEND_URL ?? 'http://127.0.0.1:3000';

export default defineConfig({
  testDir: path.join(__dirname, 'playwright'),
  timeout: 120_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: false,
  reporter: [
    ['list'],
    ['html', { outputFolder: 'performance/playwright-report', open: 'never' }],
  ],
  use: {
    baseURL: frontendUrl,
    trace: 'on-first-retry',
    actionTimeout: 15_000,
    navigationTimeout: 45_000,
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        baseURL: frontendUrl,
      },
    },
  ],
});

