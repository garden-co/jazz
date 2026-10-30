import assert from "node:assert/strict";
import test from "node:test";
import {
  analyzeComposerEcho,
  composerEcho,
  measureTypingComposer,
  percentile,
  receiptMetrics,
  type ComposerDriver,
} from "./typing-composer.ts";

test("a monotonic echo of every keystroke is clean", () => {
  assert.deepEqual(analyzeComposerEcho("abc", ["a", "ab", "abc"]), {
    dropped: 0,
    reordered: 0,
    reappeared: 0,
  });
  // Coalesced frames (a burst echoed in one frame) are not drops.
  assert.deepEqual(analyzeComposerEcho("abcd", ["a", "abcd"]), {
    dropped: 0,
    reordered: 0,
    reappeared: 0,
  });
});

test("the #2964 flicker (last character disappears then reappears) is counted", () => {
  assert.deepEqual(analyzeComposerEcho("abc", ["a", "ab", "a", "ab", "abc"]), {
    dropped: 0,
    reordered: 0,
    reappeared: 1,
  });
});

test("missing and reordered characters are counted", () => {
  assert.equal(analyzeComposerEcho("abc", ["a", "ab"]).dropped, 1);
  assert.equal(analyzeComposerEcho("abc", ["a", "ba"]).reordered, 1);
  assert.equal(analyzeComposerEcho("abc", []).dropped, 3);
});

test("percentiles use nearest rank", () => {
  assert.equal(percentile([], 0.5), 0);
  assert.equal(percentile([5, 1, 3, 2, 4], 0.5), 3);
  assert.equal(percentile([5, 1, 3, 2, 4], 0.95), 5);
});

function fakeDriver(options: { echo?: (text: string, observed: string[]) => void } = {}) {
  let time = 0;
  const observed: string[] = [];
  const pending: string[] = [];
  const driver: ComposerDriver = {
    async open() {
      return "row";
    },
    type(_id, text) {
      time += 2;
      pending.push(text);
    },
    observedTexts: () => observed,
    now: () => time,
    async yieldTurn() {
      time += 5;
      const last = pending.splice(0).at(-1);
      if (last !== undefined) (options.echo ?? ((text, into) => into.push(text)))(last, observed);
    },
  };
  return driver;
}

test("measures blocked time, echo latency and a coalesced burst", async () => {
  const metrics = await measureTypingComposer(fakeDriver(), "ab", "cde");
  assert.equal(metrics.keystrokes, 5);
  assert.equal(metrics.burst, 3);
  assert.equal(metrics.blockedMaxMs, 2);
  assert.equal(metrics.echoP50Ms, 7);
  assert.equal(metrics.dropped + metrics.reordered + metrics.reappeared, 0);
  assert.deepEqual(Object.keys(receiptMetrics(metrics)).sort(), Object.keys(metrics).sort());
});

test("fails the receipt when a character reappears", async () => {
  let flickered = false;
  await assert.rejects(
    measureTypingComposer(
      fakeDriver({
        echo(text, observed) {
          if (text === "ab" && !flickered) {
            flickered = true;
            observed.push("ab", "a");
          }
          observed.push(text);
        },
      }),
      "abc",
      "d",
    ),
    /reappeared=1/,
  );
});

// Android device acceptance (#3273): the public insert drains ready
// subscription batches before it returns, so the frame that first contains
// the composer row can arrive before the caller knows the row's id. No later
// frame follows until the row changes.
test("a composer row delivered before its insert returns is still observed", () => {
  const echo = composerEcho();
  echo.onSnapshot([{ id: "seed", title: "seed" }]);
  echo.onSnapshot([
    { id: "seed", title: "seed" },
    { id: "composer", title: "" },
  ]);
  echo.follow("composer");
  assert.deepEqual(echo.observed, [""]);
  echo.onSnapshot([
    { id: "seed", title: "seed" },
    { id: "composer", title: "h" },
  ]);
  assert.deepEqual(echo.observed, ["", "h"]);
});
