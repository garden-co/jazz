import { Page, chromium, Browser } from '@playwright/test';
import { firstCoValue } from '../../src/util';
import logger from '../../src/util/logger';

export const SERVER_URL = 'https://localhost:3000';
export const NUM_BROWSERS: number = 10;

interface BrowserInstance {
    browser: Browser;
    page: Page;
    ua: String;
}

export function getRandomCoValueIndex(): number {
    // return Math.floor(Math.random() * 5) + 1;
    return firstCoValue.index;
}

async function setupBrowser(url: string, ua: string, identifier: string = "", headless: boolean): Promise<BrowserInstance> {
    const browser: Browser = await chromium.launch({ headless });
    const page: Page = await browser.newPage();
    await page.goto(url);
    logger.debug(`Visiting URL in ${identifier}-[client-#${ua}]: ${url}`);
    // Wait for the page to load ...
    await page.waitForSelector('body', { state: 'attached' });
    
    return { browser, page, ua };
}

export async function spawnBrowsers(uuid: string, isBinary: boolean, identifier: string = "", headless: boolean = true) {
    const url = `${SERVER_URL}?uuid=${uuid}&binary=${isBinary}`;
    const browserPromises: Promise<BrowserInstance>[] = [];
    for (let i = 0; i < NUM_BROWSERS; i++) {
        const ua = i+1;
        browserPromises[i] = setupBrowser(`${url}&ua=${ua}`, `${ua}`, identifier, headless);
    }  
    const browsers: BrowserInstance[] = await Promise.all(browserPromises);
    logger.debug(`All ${NUM_BROWSERS} browsers have loaded ${url}`);
    return browsers;
}
