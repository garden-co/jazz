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
            await step(`${context.scenario.name}.load_multiple_duration`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await loadMultipleCoValues(concurrency, false);
                }, concurrency); 

                // Record the metrics
                const affix = `${concurrency}`.padStart(3, "0");
                events.emit('counter', `${context.scenario.name}.load_bulk_duration_for_${affix}`, result.duration);
                events.emit('counter', `${context.scenario.name}.load_bulk_failures_for_${affix}`, result.failed);

                logger.info(`Load multiple CoValues test for 'text ... multiple_${affix}' completed in ${result.duration}ms with ${result.failed} failures`);
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
            await step(`${context.scenario.name}.create_multiple_duration`, async () => {
                const result = await page.evaluate(async (concurrency) => {
                    return await createMultipleCoValues(concurrency, false);
                }, concurrency); 

                // Record the metrics
                const affix = `${concurrency}`.padStart(3, "0");
                events.emit('counter', `${context.scenario.name}.create_bulk_duration_for_${affix}`, result.duration);
                events.emit('counter', `${context.scenario.name}.create_bulk_failures_for_${affix}`, result.failed);

                logger.info(`Create multiple CoValues test for 'text ... multiple_${affix}' completed in ${result.duration}ms with ${result.failed} failures`);
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

            // The code below executes much faster than identical code in `artillery-binary.ts`
            // since 0.05kB (text) << 100KB (binary) hence the slight deviation in how RPS (rates) are computed
            let iterations = 10;
            while (iterations > 0) {
                // Track the start time
                const mutationStartTime = Date.now();

                // Produce a mutation
                await page.click('#mutateCoValueText');
                events.emit('counter', `${context.scenario.name}.mutation_producer_attempts`, 1);

                await page.waitForSelector('#status >> text=Mutated (text) data for:');
                events.emit('counter', `${context.scenario.name}.mutation_producer_sent`, 1);

                // Mutation completed, calculate time taken
                const mutationEndTime = Date.now();
                const mutationDuration = (mutationEndTime - mutationStartTime) / 1000; // i.e. Seconds

                // Emit rate metric for mutations per second (1 mutation / time taken)
                if (mutationDuration > 0) {
                    const rps = 1 / mutationDuration;
                    events.emit('rate', `${context.scenario.name}.mutation_producter_rate`, rps);
                }

                // Consume the mutation by checking all spawned browsers received the mutation event
                let subscribersReceived = 0;
                await Promise.all(browsers.map(async ({ page: clientPage, ua }, index) => {
                    events.emit('counter', `${context.scenario.name}.mutation_consumers`, 1);
                    await clientPage.waitForSelector(`#status >> text=Mutation event`);
                    subscribersReceived++;
                    logger.debug(`Browser ${context.vars.$uuid}-[client-${ua}] received the mutation event.`);
                }));

                // Calculate subscribers per second rate
                const totalDuration = (Date.now() - mutationStartTime) / 1000;
                if (totalDuration > 0 && subscribersReceived > 0) {
                    const subscribersPerSecond = subscribersReceived / totalDuration;
                    events.emit('rate', `${context.scenario.name}.mutation_consumers_rate`, subscribersPerSecond);
                }

                iterations--;
            }

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
        name: "1a Text - Load Multiple",
        engine: 'playwright',
        testFunction: loadMultiple
    },
    {
        name: "1b Text - Load Single",
        engine: 'playwright',
        testFunction: loadSingle
    },
    {
        name: "2c Text - Create Multiple",
        engine: 'playwright',
        testFunction: createMultiple
    },
    {
        name: "2d Text - Create Single",
        engine: 'playwright',
        testFunction: createSingle
    },
    {
        name: "3e Text - Mutate Single",
        engine: 'playwright',
        testFunction: mutateSingle
    }
];
