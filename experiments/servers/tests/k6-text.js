import { browser } from 'k6/browser';
import { sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Trend, Counter } from 'k6/metrics';
import { check } from 'https://jslib.k6.io/k6-utils/1.5.0/index.js';

export const SERVER_URL = 'https://localhost:3000';
const COVALUE1 = "b108a08f-1663-4755-b254-5bd07e5c5074";
const COVALUE2 = "c9178c9c-853f-489c-8437-9ee4f23c8885";
const shared = new SharedArray('shared state', function() {
  return [{ selectedUUID: COVALUE1 }, { selectedUUID: COVALUE2 }];
});

const metrics = {
  connectionTime: new Trend('covalue_pageload_time', true),
  loadTime: new Trend('covalue_load_time', true),
  createTime: new Trend('covalue_create_time', true),
  mutationTime: new Trend('covalue_mutation_time', true),
  mutationEventsReceived: new Counter('covalue_mutations_received'),
  mutatedEventsLost: new Counter('covalue_mutations_lost')
};

export const options = {
  scenarios: {
    browser: {
      executor: 'per-vu-iterations',
      options: {
        browser: {
          type: 'chromium',
        },
      },
      vus: 1,
      iterations: 1,
      maxDuration: '30s'
    },
    // Main browser doing the mutation
    mutator: {
      executor: 'per-vu-iterations',
      options: {
        browser: {
          type: 'chromium',
        },
      },
      vus: 1,
      iterations: 1,
      exec: 'mutatorFunction',
      startTime: '1s'
    },
    // 10 browsers watching for mutations
    watchers: {
      executor: 'per-vu-iterations',
      options: {
        browser: {
          type: 'chromium',
        },
      },
      vus: 10,
      iterations: 1,
      exec: 'watcherFunction',
      startTime: '1s'
    }
  }
};

export default async function() {
  const page = await browser.newPage();
  let requestStart;
  
  try {
    const loadTest = async () => {

      requestStart = Date.now();
      await page.goto(SERVER_URL);
      
      const statusLoaded = await page.locator('#status').textContent();
      check(statusLoaded, {
        '1.0. Connection established': (el) => {
          const connected = el.includes('CoValue UUIDs loaded successfully.');
          if (connected) {
            metrics.connectionTime.add(Date.now() - requestStart);
          }
          return connected;
        }
      });

      // Load CoValue
      await page.locator('#coValueSelect').selectOption( shared[0].selectedUUID );

      requestStart = Date.now();
      await page.locator('#loadCoValueText').click();

      const loadStatus = await page.locator('#status').textContent();
      check(loadStatus, {
          '1.1. CoValue loaded successfully': (el) => {
            const loaded = el.includes('Loaded (JSON) data for:');
            if (loaded) {
              metrics.loadTime.add(Date.now() - requestStart);
            }
            return loaded;
          }
      });

    };

    const createTest = async () => {
      // Create a CoValue
      const optionsCount = await page.evaluate(() => {
        return document.querySelector('#coValueSelect').options.length;
      });
      
      requestStart = Date.now();
      await page.click('#createCoValueText');

      const newOptionsCount = await page.evaluate(() => {
        return document.querySelector('#coValueSelect').options.length;
      });
      check(page, {
        '2.0. CoValue created successfully': async () => {
          const status = page.locator('#status');
          const created = (await status.textContent()).includes('Created (JSON) data for:');
          if (created) {
            metrics.createTime.add(Date.now() - requestStart);
          }
          return created;
        },
        '2.1. CoValues list increased by 1': () => {
          return newOptionsCount === optionsCount + 1;
        }
      });
    };

    // Run all tests
    await loadTest();
    await createTest();
    
  } finally {
      await page.close();
  }
}

export async function mutatorFunction() {
  const page = await browser.newPage();
  // const isBinary = false;
  // const uuid = shared[1].selectedUUID;
  // console.log(`Watcher ${__VU} selecting CoValue: ${uuid}`);

  try {
    await page.goto(SERVER_URL);
      
    const statusLoaded = await page.locator('#status').textContent();
    check(statusLoaded, {
      '3.0. Connection established': (el) => el.includes('CoValue UUIDs loaded successfully.'),
    });

    // FIXME: Select random CoValue
    await page.locator('#coValueSelect').selectOption( shared[1].selectedUUID );
    // sleep(2);
      
    // Mutate CoValue
    const mutationStart = Date.now();
    await page.click('#mutateCoValueText');
    const mutateStatus = await page.locator('#status').textContent();
    check(mutateStatus, {
      '3.1. CoValue mutated successfully': (el) => {
        const mutated = el.includes('Mutated (JSON) data for:');
        if (mutated) {
          metrics.mutationTime.add(Date.now() - mutationStart);
        }
        return mutated;
      }
    });
  
  } finally {
    await page.close();
  }
}

export async function watcherFunction() {
  const page = await browser.newPage();
  const isBinary = false;

  try {
    const uuid = shared[1].selectedUUID;
    const ua = __VU;
    const url = `${SERVER_URL}?uuid=${uuid}&binary=${isBinary}&ua=${ua}`;
    console.log(`Watcher [client-#${ua}] visiting URL: ${url}`);

    await page.goto(url);
    sleep(2);

    const mutateStatus = await (await page.waitForSelector("#status")).textContent();
    //page.locator('#status').textContent();
    check(mutateStatus, {
        '4.1. CoValue mutation received successfully': (el) => {
          const received = el.includes('Mutation event')
          if (received) {
            metrics.mutationEventsReceived.add(1);
          } else {
            metrics.mutatedEventsLost.add(1);
          }
          return received;
        }
    });
  
  } finally {
    await page.close();
  }
}
