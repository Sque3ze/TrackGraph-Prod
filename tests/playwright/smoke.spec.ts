import { test, expect } from '@playwright/test';

const backendUrl = process.env.BACKEND_URL ?? 'http://127.0.0.1:8000';

test.describe('TrackGraph smoke', () => {
  test.beforeEach(async () => {
    await fetch(`${backendUrl}/api/use_default_history`, { method: 'POST' }).catch(() => {});
  });

  test('loads bubbles and leaderboard views', async ({ page }) => {
    await page.goto('/');

    const sampleButton = page.getByRole('button', { name: /Load demo/i });
    await expect(sampleButton).toBeVisible();
    await sampleButton.click();

    await page.locator('.bubbles-gate').waitFor({ state: 'detached' });

    const bubbleNode = page.locator('circle.bubble-node').first();
    await expect(bubbleNode).toBeVisible();
    await bubbleNode.click();

    const detailsPanel = page.locator('.details-panel');
    await expect(detailsPanel).toBeVisible();

    const clearButton = page.getByRole('button', { name: /Clear selection/i });
    await expect(clearButton).toBeVisible();
    await clearButton.click();
    await expect(detailsPanel).toBeHidden();

    // Switch to album bubbles and ensure nodes still render
    const group = page.getByRole('radiogroup', { name: 'Group bubbles by' });
    await expect(group).toBeVisible();


    const albumsLabel = group.locator('label.group-option', { hasText: 'Albums' });
    await albumsLabel.scrollIntoViewIfNeeded();
    await albumsLabel.click();

    await expect(page.locator('circle.bubble-node').first()).toBeVisible();

    // Navigate to leaderboard view
    await page.getByRole('button', { name: /top stats/i }).click();
    await expect(page.getByRole('heading', { name: /top artists/i })).toBeVisible();
    await expect(page.getByRole('heading', { name: /top albums/i })).toBeVisible();
    await expect(page.getByRole('heading', { name: /top tracks/i })).toBeVisible();
  });
});
