import assert from "node:assert/strict";
import test from "node:test";
import { createDeviceDiagnosticTracker } from "./diagnostic-lifecycle.ts";

test("a synchronous diagnostic sink failure cannot replace the native boundary", () => {
  const tracker = createDeviceDiagnosticTracker(
    () => {
      throw new Error("planted secret-bearing sink failure");
    },
    async () => {},
  );
  assert.doesNotThrow(() => tracker.mark("foreground-open-failed"));
});

test("a never-settling diagnostic write cannot delay the marked native call", () => {
  const observed: string[] = [];
  const tracker = createDeviceDiagnosticTracker(
    (code) => {
      observed.push(code);
      return new Promise(() => {});
    },
    async () => {},
  );
  tracker.mark("foreground-probe-failed");
  observed.push("native-call-entered");
  assert.deepEqual(observed, ["foreground-probe-failed", "native-call-entered"]);
});

test("a rejected diagnostic write is contained and retryable before native entry", async () => {
  const observed: string[] = [];
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);
  try {
    const tracker = createDeviceDiagnosticTracker(
      async (code) => {
        observed.push(code);
        throw new Error("planted secret-bearing rejected sink");
      },
      async () => {},
    );
    tracker.mark("foreground-install-failed");
    observed.push("native-call-entered");
    tracker.retry();
    await new Promise<void>((resolve) => setImmediate(resolve));

    assert.deepEqual(observed, [
      "foreground-install-failed",
      "native-call-entered",
      "foreground-install-failed",
    ]);
    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("successful lifecycle clearing runs only through the native clear boundary", async () => {
  const observed: string[] = [];
  const tracker = createDeviceDiagnosticTracker(
    async (code) => {
      observed.push(code);
    },
    async () => {
      observed.push("cleared");
    },
  );
  tracker.mark("foreground-close-failed");
  await tracker.clear();
  assert.deepEqual(observed, ["foreground-close-failed", "cleared"]);
  assert.doesNotMatch(observed.join("\n"), /capability|token|error/i);
});
