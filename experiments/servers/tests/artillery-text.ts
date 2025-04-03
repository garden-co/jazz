import { Page, expect } from '@playwright/test';
import logger from '../src/util/logger';
import { SERVER_URL, getRandomCoValueIndex, spawnBrowsers, concurrencyLevels1 as concurrencyLevels, getPID } from './common';

async function loadMultiple(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target, { timeout: 55_000 });
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        for (const concurrency of concurrencyLevels) {
            await step(`${context.scenario.name}.load_text_duration_${concurrency}multiple`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await loadMultipleCoValues(concurrency, false);
                }, concurrency); 

                // Record the metrics
                events.emit('histogram', `load_text_duration_${concurrency}multiple`, result.duration);
                events.emit('histogram', `load_text_failure_${concurrency}multiple`, result.failed);

                logger.info(`Load multiple CoValues test for 'text_${concurrency}multiple' completed in ${result.duration}ms with ${result.failed} failures`);
            });
        }
    } catch (error) {
        logger.error('Load Test error:', error);
        throw error;
    }
}

async function loadSingle(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });

        const randomIndex = getRandomCoValueIndex();
        await step(`${context.scenario.name}.load_text_duration`, async () => {
            await page.selectOption('select#coValueSelect', { index: randomIndex });
            await page.click('#loadCoValueText');
            await page.waitForSelector('#status >> text=Loaded (text) data for:', { timeout: 50000 });
        });

    } catch (error) {
        logger.error('Load Test error:', error);
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

        for (const concurrency of concurrencyLevels) {
            await step(`${context.scenario.name}.create_text_duration_${concurrency}multiple`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await createMultipleCoValues(concurrency, false);
                }, concurrency); 

                // Record the metrics
                events.emit('histogram', `create_text_duration_${concurrency}multiple`, result.duration);
                events.emit('histogram', `create_text_failure_${concurrency}multiple`, result.failed);

                logger.info(`Create multiple CoValues test for 'text_${concurrency}multiple' completed in ${result.duration}ms with ${result.failed} failures`);
            });
        }
    } catch (error) {
        logger.error('Create Test error:', error);
        throw error;
    }
}

async function createSingle(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });
        
        const initialOptions = await page.locator('select#coValueSelect option').all();
        await step(`${context.scenario.name}.create_text_duration`, async () => {
            await page.click('#createCoValueText');
            await page.waitForSelector('#status >> text=Created (text) data for:', { timeout: 50000 });
        });
        const newOptions = await page.locator('select#coValueSelect option').all();
        expect(newOptions.length).toEqual(initialOptions.length + 1);
    } catch (error) {
        logger.error('Create Test error:', error);
        throw error;
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

        await step(`${context.scenario.name}.mutate_text_duration`, async () => {
            await page.selectOption('select#coValueSelect', { index: randomIndex });
            const uuid = await page.locator('select#coValueSelect').evaluate((el: HTMLSelectElement) => el.value);

            // Spawn the additional browsers for mutation events
            const browsers = await spawnBrowsers(uuid, false, context.vars.$uuid);

            // Perform mutation
            await page.click('#mutateCoValueText');
            events.emit('counter', `${context.scenario.name}.mutate_text_sent`, 1);

            await page.waitForSelector('#status >> text=Mutated (text) data for:');
            events.emit('counter', `${context.scenario.name}.mutate_text_delivered`, 1);

            // Check all spawned browsers received the mutation event
            await Promise.all(browsers.map(async ({ page: clientPage, ua }, index) => {
                await clientPage.waitForSelector(`#status >> text=Mutation event`);
                events.emit('counter', `${context.scenario.name}.mutate_text_subscriber`, 1);
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
        }
    },
    phases: [{
        duration: 20, // 20 seconds
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
                duration: 180, // 3 minutes
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
        name: "1a Load Multiple",
        engine: 'playwright',
        testFunction: loadMultiple
    },
    {
        name: "1b Load Single",
        engine: 'playwright',
        testFunction: loadSingle
    },
    {
        name: "2c Create Multiple",
        engine: 'playwright',
        testFunction: createMultiple
    },
    {
        name: "2d Create Single",
        engine: 'playwright',
        testFunction: createSingle
    },
    {
        name: "3e Mutate Single",
        engine: 'playwright',
        testFunction: mutateSingle
    }
];
