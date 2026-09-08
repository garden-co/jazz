import test from "node:test";
import assert from "node:assert/strict";
import { chromium } from "playwright";
import { evaluateHarnessOperation } from "../tests/browser/evaluate-harness.mjs";

const modulePath =
  "data:text/javascript," +
  encodeURIComponent(`
export function wait() {
  globalThis.starts = (globalThis.starts ?? 0) + 1;
  let finish;
  const operation = new Promise(resolve => { finish = resolve; });
  operation.finish = finish;
  globalThis.operationRef = new WeakRef(operation);
  return operation;
}
export function fail() { throw new TypeError('synthetic operation failure'); }
`);

// Both transports execute the identical operation. Its producer can complete it
// through a WeakRef while it is alive; it deliberately delegates strong lifetime
// ownership to the caller. GC must not turn an active operation into navigation.
for (const transport of ["legacy", "registry"]) {
  test(`${transport}: pending operation survives only with page-owned retention`, async () => {
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      const cdp = await page.context().newCDPSession(page);
      const pending = (
        transport === "legacy"
          ? page.evaluate(
              async ({ modulePath }) => {
                const harness = await import(modulePath);
                return harness.wait();
              },
              { modulePath },
            )
          : evaluateHarnessOperation(page, modulePath, "wait", null, 5_000)
      ).then(
        (value) => ({ value }),
        (error) => ({ error }),
      );
      await page.waitForFunction(() => !!globalThis.operationRef);
      await cdp.send("HeapProfiler.collectGarbage");
      const alive = await page.evaluate(() => {
        const operation = globalThis.operationRef.deref();
        operation?.finish([{ title: "expected" }]);
        return !!operation;
      });
      const result = await pending;
      assert.equal(await page.evaluate(() => globalThis.starts), 1);
      assert.equal(page.isClosed(), false);
      if (transport === "legacy") {
        assert.equal(alive, false);
        assert.match(result.error.message, /promise was garbage collected|context was destroyed/i);
      } else {
        assert.equal(alive, true);
        assert.deepEqual(result, { value: [{ title: "expected" }] });
        assert.equal(await page.evaluate(() => globalThis.__jazzHarnessOperations__.size), 0);
      }
    } finally {
      await browser.close();
    }
  });
}

test("registry preserves failure and removes timed-out operations", async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await assert.rejects(evaluateHarnessOperation(page, modulePath, "fail", null), {
      name: "TypeError",
      message: "synthetic operation failure",
    });
    assert.equal(await page.evaluate(() => globalThis.__jazzHarnessOperations__.size), 0);
    await assert.rejects(
      evaluateHarnessOperation(page, modulePath, "wait", null, 100),
      /did not settle/,
    );
    assert.equal(await page.evaluate(() => globalThis.__jazzHarnessOperations__.size), 0);
  } finally {
    await browser.close();
  }
});

test("registry retains timer-rooted title delivery through forced GC", async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const cdp = await page.context().newCDPSession(page);
    // Same timeout + retained delivery-callback shape as the remote title waiter.
    // This complements the planted unrooted positive; it is not a Jazz replay.
    const timerModule =
      "data:text/javascript," +
      encodeURIComponent(`
      export async function wait() {
        return await new Promise((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('title timeout')), 5000);
          globalThis.deliver = (rows) => {
            if (rows.some(row => row.title === 'expected')) {
              clearTimeout(timeout);
              delete globalThis.deliver;
              resolve(rows);
            }
          };
        });
      }
    `);
    const pending = evaluateHarnessOperation(page, timerModule, "wait", null);
    await page.waitForFunction(() => typeof globalThis.deliver === "function");
    await cdp.send("HeapProfiler.collectGarbage");
    await page.evaluate(() => globalThis.deliver([{ title: "expected" }]));
    assert.deepEqual(await pending, [{ title: "expected" }]);
    assert.equal(await page.evaluate(() => globalThis.__jazzHarnessOperations__.size), 0);
  } finally {
    await browser.close();
  }
});

test("registry reports missing methods/modules and page closure", async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await assert.rejects(
      evaluateHarnessOperation(page, modulePath, "missing", null),
      /unavailable/,
    );
    await assert.rejects(
      evaluateHarnessOperation(
        page,
        'data:text/javascript,throw new Error("module failure")',
        "wait",
        null,
      ),
      /module failure/,
    );
    assert.equal(await page.evaluate(() => globalThis.__jazzHarnessOperations__.size), 0);
    const pending = evaluateHarnessOperation(page, modulePath, "wait", null);
    const closed = assert.rejects(pending, /closed/);
    await page.waitForFunction(() => !!globalThis.operationRef);
    await page.close();
    await closed;
  } finally {
    await browser.close();
  }
});
