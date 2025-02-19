import { Page, expect } from '@playwright/test';
import logger from '../src/util/logger';
import { SERVER_URL, getRandomCoValueIndex, spawnBrowsers } from './common';


async function runTextLoadTest(page: Page, context: any, events: any, test: any) {
    const { step } = test;
//   const oldContext = page.context;
//   page.context = () => { 
//     ['onLCP', 'onFCP', 'onCLS', 'onTTFB', 'onFID', 'onINP'].forEach(
//           (hook) => {
//             page.context().removeAllListeners(hook, {behavior: 'ignoreErrors'});
//           }
//     );
//     return oldContext();
//   };

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
                events.emit('counter', `${context.scenario.name}.mutate_text_event_delivered`, 1);
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
        duration: 20,
        arrivalRate: 1,
        maxVusers: 1,
        // rampTo: 1,
        name: "CoValue initial load testing"
    }]
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