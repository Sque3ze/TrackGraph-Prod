import fs from 'node:fs/promises';
import path from 'node:path';
import { test } from '@playwright/test';

const backendUrl = process.env.BACKEND_URL ?? 'http://127.0.0.1:8000';

async function ensureSampleData() {
  try {
    await fetch(`${backendUrl}/api/use_default_history`, { method: 'POST' });
  } catch (err) {
    console.warn('Unable to enable default dataset before performance test:', err);
  }
}

test.describe('performance capture', () => {
  test.beforeAll(async () => {
    await ensureSampleData();
    await fs.mkdir(path.join(process.cwd(), 'performance'), { recursive: true });
  });

  test('records frontend resource timings and backend timings', async ({ page }, testInfo) => {
    await page.goto('/');

    const sampleButton = page.getByRole('button', { name: /Load demo/i });
    await sampleButton.click();

    await page.waitForSelector('circle.bubble-node');

    // Gather frontend metrics
    const frontendMetrics = await page.evaluate(() => {
      const navEntries = performance.getEntriesByType('navigation').map((entry) => ({
        name: entry.name,
        type: entry.type,
        startTime: entry.startTime,
        duration: entry.duration,
        domContentLoaded: entry.domContentLoadedEventEnd,
        loadEventEnd: entry.loadEventEnd,
      }));

      const imageResources = performance
        .getEntriesByType('resource')
        .filter((entry) => entry.initiatorType === 'img')
        .map((entry) => ({
          name: entry.name,
          startTime: entry.startTime,
          duration: entry.duration,
          transferSize: entry.transferSize ?? null,
        }));

      return { navigation: navEntries, images: imageResources };
    });

    // Collect backend timings from key endpoints
    const backendCalls = [
      ['/api/bubbles?group_by=artist', 'bubbles_artist'],
      ['/api/bubbles?group_by=album', 'bubbles_album'],
      ['/api/summary', 'summary'],
      ['/api/historical_data?limit=200', 'historical'],
    ];

    const backendMetrics = {} as Record<string, unknown>;

    for (const [pathFragment, label] of backendCalls) {
      try {
        const res = await fetch(`${backendUrl}${pathFragment}`);
        const json = await res.json();
        backendMetrics[label] = json?.timings ?? null;
      } catch (err) {
        backendMetrics[label] = { error: String(err) };
      }
    }

    const timestamp = new Date().toISOString().replace(/[:]/g, '-');
    const payload = {
      capturedAt: new Date().toISOString(),
      frontend: frontendMetrics,
      backend: backendMetrics,
    };

    const filePath = path.join(process.cwd(), 'performance', `metrics-${timestamp}.json`);
    await fs.writeFile(filePath, JSON.stringify(payload, null, 2), 'utf-8');

    await testInfo.attach('performance-metrics', {
      path: filePath,
      contentType: 'application/json',
    });
  });
});

