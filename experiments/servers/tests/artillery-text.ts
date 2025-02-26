import { Page, expect } from '@playwright/test';
import logger from '../src/util/logger';
import { SERVER_URL, getRandomCoValueIndex, spawnBrowsers } from './common';


async function runTextLoadTest(page: Page, context: any, events: any, test: any) {
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
            await page.waitForSelector('#status >> text=Loaded (JSON) data for:');
        });

    } catch (error) {
        logger.error('Load Test error:', error);
        throw error;
    }
}

async function runTextCreateTest(page: Page, context: any, events: any, test: any) {
    const { step } = test;
    try {
        await step(`${context.scenario.name}.load_page_duration`, async () => {
            await page.goto(context.vars.target);
            await page.waitForSelector('#status >> text=CoValue UUIDs loaded successfully.');
        });
        
        const initialOptions = await page.locator('select#coValueSelect option').all();
        await step(`${context.scenario.name}.create_text_duration`, async () => {
            await page.click('#createCoValueText');
            await page.waitForSelector('#status >> text=Created (JSON) data for:');
        });
        const newOptions = await page.locator('select#coValueSelect option').all();
        expect(newOptions.length).toEqual(initialOptions.length + 1);
    } catch (error) {
        logger.error('Create Test error:', error);
        throw error;
    }
}

async function runTextMutateTest(page: Page, context: any, events: any, test: any) {
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

            await page.waitForSelector('#status >> text=Mutated (JSON) data for:');
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

function getPID(): number {
    return process.env.PID ? parseInt(process.env.PID, 10) : 0;
}

export {
  runTextLoadTest,
  runTextCreateTest,
  runTextMutateTest,
  spawnBrowsers
};
 
export const config = {
    target: SERVER_URL,
    engines: {
        playwright: { aggregateByName: true }
    },
    phases: [{
        duration: 20, // 20 seconds
        arrivalCount: 1, // 1 vuser only
        maxVusers: 1, // 1 vuser maximum
        name: "Development testing"
    }],
    plugins: {
        "memory-inspector": [
          { pid: getPID(), name: "web-server-stats", unit: 'mb' }
        ]
    },
    environments: {
        simulation: {
            phases: [
            {
                duration: 60, // 60 seconds
                arrivalRate: 5, // 5 vusers/second
                rampTo: 15, // Ramp up to 15 vusers/second
                name: "01 Warmup - gradually increase load"
            },
            {
                duration: 120, // 2 minutes
                arrivalRate: 15, // 15 vusers/second
                name: "02 Steady - maintain moderate load"
            },
            {
                duration: 30, // 30 seconds
                arrivalRate: 50, // Spike to 50 vusers/second
                name: "03 Spike - simulate peak traffic"
            },
            {
                duration: 180, // 3 minutes
                arrivalRate: 20, // Start at 20 vusers/second
                rampTo: 40, // Ramp up to 40 vusers/second
                name: "04 Scale - stress test at high load"
            },
            {
                duration: 90, // 90 seconds
                arrivalRate: 15, // Start at 15 vusers/second
                rampTo: 5, // Taper down to 5 vusers/second
                name: "05 Cooldown - reduce load gracefully"
            }
            ]
        }
    }
};

export const scenarios = [
    {
        name: "01 Load Scenario",
        engine: 'playwright',
        testFunction: runTextLoadTest
    },
    {
        name: "02 Create Scenario",
        engine: 'playwright',
        testFunction: runTextCreateTest
    },
    {
        name: "03 Mutate Scenario",
        engine: 'playwright',
        testFunction: runTextMutateTest
    }
];


// export function $rewriteMetricName(metricName: string, metricType: string) {
//     if (metricName.includes('/checkout?promoid=')) {
//     return 'browser.page.checkout';
//     } else {
//     return metricName;
//     }
// }