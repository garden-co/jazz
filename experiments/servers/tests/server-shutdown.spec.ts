import { test, expect } from '@playwright/test';
import { SERVER_URL } from './common';

test.describe('Server Shutdown', () => {
  test('should gracefully shutdown server', async ({ context, request }) => {
    await context.setExtraHTTPHeaders({});

    const response = await request.post(`${SERVER_URL}/stop`, {
      headers: {
        'Content-Type': 'text/plain; charset=utf-8'
      }
    });

    expect(response.ok()).toBeTruthy();
    
    const data = await response.json();
    expect(data).toHaveProperty('m', 'Performance data written to CSV. Server shutting down.');
  });
});