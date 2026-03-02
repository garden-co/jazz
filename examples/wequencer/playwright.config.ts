import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
	testDir: './e2e',
	fullyParallel: true,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 2 : 0,
	workers: 1,
	timeout: 90_000,
	reporter: 'html',
	globalSetup: './e2e/global-setup.ts',
	use: {
		baseURL: 'http://localhost:5175',
		trace: 'on-first-retry',
	},
	projects: [
		{
			name: 'chromium',
			use: { ...devices['Desktop Chrome'] },
		},
	],
	webServer: {
		command: 'npx vite dev --port 5175',
		port: 5175,
		reuseExistingServer: !process.env.CI,
		env: {
			VITE_JAZZ_SERVER_URL: `http://127.0.0.1:19878`,
			VITE_JAZZ_SERVER_PORT: '19878',
			VITE_JAZZ_APP_ID: '00000000-0000-0000-0000-000000000099',
			VITE_E2E: 'true',
		},
	},
});
