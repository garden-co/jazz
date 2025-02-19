import { Page, expect } from '@playwright/test';
import logger from '../src/util/logger';
import { SERVER_URL, getRandomCoValueIndex, spawnBrowsers } from './common';
import fs from 'fs';
import path from 'path';

 
function cleanUp() {
    const testFilesDir = path.join(__dirname, '../../public/uploads/');
    if (fs.existsSync(testFilesDir)) {
        fs.readdirSync(testFilesDir).forEach((file) => {
            const filePath = path.join(testFilesDir, file);
            if (fs.lstatSync(filePath).isFile()) {
                fs.unlinkSync(filePath);
            }
        });
    }
}

async function runBinaryLoadTest(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        const randomIndex = getRandomCoValueIndex();
        await step(`${context.scenario.name}.load_binary_duration`, async () => {
            await page.selectOption('select#coValueSelect', { index: randomIndex });
            await page.click('#loadCoValueBinary');
            await page.waitForSelector('#status >> text=Loaded (binary) data for:');
        });

    } catch (error) {
        logger.error('Load Test error:', error);
        throw error;
    }
}

async function runBinaryCreateTest(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });
        
        const initialOptions = await page.locator('select#coValueSelect option').all();
       
        // Pick a binary file for upload
        const filePath = path.resolve(__dirname, '../fixtures/binary-sample.zip');
        await page.locator('#fileInput').setInputFiles(filePath);
        
        await step(`${context.scenario.name}.create_binary_duration`, async () => {
            await page.click('#createCoValueBinary');
            await page.waitForSelector('#status >> text=Created (binary) data for:');
        });
        const newOptions = await page.locator('select#coValueSelect option').all();
        expect(newOptions.length).toEqual(initialOptions.length + 1);
    } catch (error) {
        logger.error('Create Test error:', error);
        throw error;
    } finally {
        cleanUp();
    }
}

async function runBinaryMutateTest(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {

        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        const randomIndex = getRandomCoValueIndex();

        await step(`${context.scenario.name}.mutate_binary_duration`, async () => {
            await page.selectOption('select#coValueSelect', { index: randomIndex });
            const uuid = await page.locator('select#coValueSelect').evaluate((el: HTMLSelectElement) => el.value);

            // Spawn the additional browsers for mutation events
            const browsers = await spawnBrowsers(uuid, true, context.vars.$uuid);

            // Perform mutation
            await page.click('#mutateCoValueBinary');
            events.emit('counter', `${context.scenario.name}.mutate_binary_sent`, 1);

            await page.waitForSelector('#status >> text=Mutated (binary) data for:');
            events.emit('counter', `${context.scenario.name}.mutate_binary_delivered`, 1);

            // Check all spawned browsers received the mutation event
            await Promise.all(browsers.map(async ({ page: clientPage, ua }, index) => {
                await clientPage.waitForSelector(`#status >> text=Mutation event`);
                events.emit('counter', `${context.scenario.name}.mutate_binary_event_delivered`, 1);
                logger.debug(`Browser ${context.vars.$uuid}-[client-${ua}] received the mutation event.`);
            }));

            // Cleanup spawned browsers
            await Promise.all(browsers.map(async ({ browser, ua }) => {
                await browser.close();
                logger.debug(`Browser ${context.vars.$uuid}-[client-${ua}] closed`);
            }));
        });
    } catch (error) {
        logger.error('Mutate Test error:', error);
        throw error;
    }
}

export {
  runBinaryLoadTest,
  runBinaryCreateTest,
  runBinaryMutateTest,
  spawnBrowsers
};
 
export const config = {
    target: SERVER_URL,
    engines: {
        playwright: { aggregateByName: true }
    },
    phases: [{
        duration: 30,
        arrivalRate: 1,
        maxVusers: 1,
        // rampTo: 1,
        name: "CoValue initial load testing"
    }]
};

export const scenarios = [
    {
        name: "04 Load Scenario",
        engine: 'playwright',
        testFunction: runBinaryLoadTest
    },
    {
        name: "05 Create Scenario",
        engine: 'playwright',
        testFunction: runBinaryCreateTest
    },
    {
        name: "06 Mutate Scenario",
        engine: 'playwright',
        testFunction: runBinaryMutateTest
    }
];
