/**
 * Execute once without asking CDP to await a long-lived browser promise.
 * Chromium may collect an awaited evaluation promise while its realm is alive.
 * Keep the operation page-owned and transfer only synchronous snapshots to Node.
 * The transport deadline bounds stalled imports/operations; a harness method keeps
 * its own (usually shorter) semantic timeout. Removing an entry is not cancellation.
 *
 * @param {import('playwright').Page} page
 * @param {string} modulePath
 * @param {string} moduleMethod
 * @param {unknown} args
 * @param {number} timeoutMs
 * @returns {Promise<any>}
 */
export async function evaluateHarnessOperation(
  page,
  modulePath,
  moduleMethod,
  args,
  timeoutMs = 60_000,
) {
  const id = crypto.randomUUID();
  const deadline = Date.now() + timeoutMs;
  try {
    await page.evaluate(
      ({ id, modulePath, moduleMethod, args }) => {
        const registry = (globalThis.__jazzHarnessOperations__ ??= new Map());
        const entry = { state: "pending" };
        registry.set(id, entry);
        entry.importPromise = import(/* @vite-ignore */ modulePath);
        entry.completion = entry.importPromise
          .then((harness) => {
            const method = harness[moduleMethod];
            if (typeof method !== "function") {
              throw new Error(`Remote browser harness method "${moduleMethod}" is unavailable`);
            }
            entry.operation = method(args);
            return entry.operation;
          })
          .then(
            (value) => {
              entry.result = { state: "fulfilled", value };
            },
            (error) => {
              entry.result = {
                state: "rejected",
                name: error instanceof Error ? error.name : "Error",
                message: error instanceof Error ? error.message : String(error),
                stack: error instanceof Error ? error.stack : undefined,
              };
            },
          );
      },
      { id, modulePath, moduleMethod, args },
    );
    while (Date.now() < deadline) {
      const result = await page.evaluate((id) => {
        const entry = globalThis.__jazzHarnessOperations__?.get(id);
        if (!entry) throw new Error("Remote browser harness operation disappeared");
        return entry.result ?? { state: "pending" };
      }, id);
      if (result.state === "fulfilled") return result.value;
      if (result.state === "rejected") {
        const error = new Error(result.message);
        error.name = result.name;
        if (result.stack) error.stack = result.stack;
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error(
      `Remote browser harness method "${moduleMethod}" did not settle within ${timeoutMs}ms`,
    );
  } finally {
    if (!page.isClosed()) {
      await page
        .evaluate((id) => globalThis.__jazzHarnessOperations__?.delete(id), id)
        .catch(() => undefined);
    }
  }
}
