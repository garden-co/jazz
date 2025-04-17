import { Page, expect } from '@playwright/test';
import logger from '../src/util/logger';
import { SERVER_URL, getRandomCoValueIndex, spawnBrowsers, concurrencyLevels2 as concurrencyLevels, getPID } from './common';
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

async function loadMultiple(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        for (const concurrency of concurrencyLevels) {
            await step(`${context.scenario.name}.load_multiple_duration`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await loadMultipleCoValues(concurrency, true); // false for text CoValues
                }, concurrency); 

                // Record the metrics
                const affix = `${concurrency}`.padStart(3, "0");
                events.emit('counter', `${context.scenario.name}.load_bulk_duration_for_${affix}`, result.duration);
                events.emit('counter', `${context.scenario.name}.load_bulk_failures_for_${affix}`, result.failed);

                logger.info(`Load multiple CoValues test for 'binary ... multiple_${affix}' completed in ${result.duration}ms with ${result.failed} failures`);
            });
        }
    } catch (error) {
        logger.error('Binary - Load multiple test error:', error);
        throw error;
    }
}

async function loadSingle(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.', { timeout: 184_000 });
        });

        const randomIndex = getRandomCoValueIndex();
        await step(`${context.scenario.name}.load_binary_duration`, async () => {
            await page.selectOption('select#coValueSelect', { index: randomIndex });
            await page.click('#loadCoValueBinary');
            await page.waitForSelector('#status >> text=Loaded (binary) data for:', { timeout: 185_000 });
        });

    } catch (error) {
        logger.error('Binary - Load single test error:', error);
        throw error;
    }
}

async function createMultiple(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        // Pick a binary file for upload
        const filePath = path.resolve(__dirname, '../fixtures/binary-sample.zip');
        await page.locator('#fileInput').setInputFiles(filePath);

        for (const concurrency of concurrencyLevels) {
            await step(`${context.scenario.name}.create_multiple_duration`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await createMultipleCoValues(concurrency, true); // false for text CoValues
                }, concurrency); 

                // Record the metrics
                const affix = `${concurrency}`.padStart(3, "0");
                events.emit('counter', `${context.scenario.name}.create_bulk_duration_for_${affix}`, result.duration);
                events.emit('counter', `${context.scenario.name}.create_bulk_failures_for_${affix}`, result.failed);

                logger.info(`Create multiple CoValues test for 'binary ... multiple_${affix}' completed in ${result.duration}ms with ${result.failed} failures`);
            });
        }
    } catch (error) {
        logger.error('Binary - Create multiple test error:', error);
        throw error;
    } finally {
        cleanUp();
    }
}

async function createSingle(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.', { timeout: 281_000 });
        });
        
        const initialOptions = await page.locator('select#coValueSelect option').all();
       
        // Pick a binary file for upload
        const filePath = path.resolve(__dirname, '../fixtures/binary-sample.zip');
        await page.locator('#fileInput').setInputFiles(filePath, { timeout: 281_000 });
        
        await step(`${context.scenario.name}.create_binary_duration`, async () => {
            await page.click('#createCoValueBinary', { timeout: 282_000 });
            await page.waitForSelector('#status >> text=Created (binary) data for:', { timeout: 283_000 });
        });
        const newOptions = await page.locator('select#coValueSelect option').all();
        expect(newOptions.length).toEqual(initialOptions.length + 1);
    } catch (error) {
        logger.error('Binary - Create single test error:', error);
        throw error;
    } finally {
        cleanUp();
    }
}

async function mutateSingle(page: Page, context: any, events: any, test: any) {
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

            let iterations = 10;
            while (iterations > 0) {
                // Produce a mutation
                await page.click('#mutateCoValueBinary');
                events.emit('counter', `${context.scenario.name}.mutation_producer_attempts`, 1);

                await page.waitForSelector('#status >> text=Mutated (binary) data for:');
                events.emit('counter', `${context.scenario.name}.mutation_producer_sent`, 1);

                events.emit('rate', `${context.scenario.name}.mutation_producer_rate`);

                // Consume the mutation by checking all spawned browsers received the mutation event
                await Promise.all(browsers.map(async ({ page: clientPage, ua }, index) => {
                    events.emit('counter', `${context.scenario.name}.mutation_consumers`, 1);
                    await clientPage.waitForSelector(`#status >> text=Mutation event`);
                    events.emit('rate', `${context.scenario.name}.mutation_consumers_rate`);
                    logger.debug(`Browser ${context.vars.$uuid}-[client-${ua}] received the mutation event.`);
                }));

                iterations--;
            }

            // Cleanup spawned browsers
            await Promise.all(browsers.map(async ({ browser, ua }) => {
                await browser.close();
                logger.debug(`Browser ${context.vars.$uuid}-[client-${ua}] closed`);
            }));
        });
    } catch (error) {
        logger.error('Binary - Mutate single test error:', error);
        throw error;
    }
}

export {
    loadMultiple,
    loadSingle,
    createMultiple,
    createSingle,
    mutateSingle,
    spawnBrowsers
};
 
export const config = {
    target: SERVER_URL,
    engines: {
        playwright: { 
            aggregateByName: true,
            launchOptions: { headless: true }
        }
    },
    phases: [{
        duration: 60, // 60 seconds
        arrivalCount: 1, // 1 vuser only
        maxVusers: 1, // 1 vuser maximum
        name: "Development testing (default)"
    }],
    plugins: {
        "memory-inspector": [
          { pid: getPID(), name: "web-server-stats", unit: 'mb' }
        ]
    },
    environments: {
        multiple: {
            phases: [
            {
                // duration: 180, // 3 minutes
                duration: 60,
                arrivalCount: 1, // 1 vuser only
                maxVusers: 1, // 1 vuser maximum
                name: "Single user, multiple (concurrent) requests"
            }]
        },
        single: {
            phases: [
            {
                duration: 60, // 1 minute
                arrivalRate: 1, // 1 vusers/second
                name: "Multiple users, single request - warmup load (01)"
            },
            {
                duration: 60, // 1 minute
                arrivalRate: 1, // 1 vusers/second
                // rampTo: 2, // Ramp up to 2 vusers/second
                name: "Multiple users, single request - steady load (02)"
            },
            ]
        }
    }
};

export const scenarios = [
    {
        name: "1a Binary - Load Multiple",
        engine: 'playwright',
        testFunction: loadMultiple
    },
    {
        name: "1b Binary - Load Single",
        engine: 'playwright',
        testFunction: loadSingle
    },
    {
        name: "2c Binary - Create Multiple",
        engine: 'playwright',
        testFunction: createMultiple
    },
    {
        name: "2d Binary - Create Single",
        engine: 'playwright',
        testFunction: createSingle
    },
    {
        name: "3e Binary - Mutate Single",
        engine: 'playwright',
        testFunction: mutateSingle
    }
];
