import assert from "node:assert/strict";
import test from "node:test";
import { waitForPublication } from "./publication-wait.ts";

test("production publication wait services an event-loop task", async () => {
  let observed = false;
  const nativeWake = new Promise<void>((resolve) => {
    setTimeout(() => {
      observed = true;
      resolve();
    }, 0);
  });

  const published = await waitForPublication(() => observed);
  await nativeWake;
  assert.equal(published, true);
});

test("publication wait yields event-loop turns until the native wake arrives", async () => {
  let turns = 0;
  const published = await waitForPublication(
    () => turns === 3,
    async () => {
      turns += 1;
    },
  );
  assert.equal(published, true);
  assert.equal(turns, 3);
});

test("publication wait fails after exactly eight bounded turns", async () => {
  let turns = 0;
  const published = await waitForPublication(
    () => false,
    async () => {
      turns += 1;
    },
  );
  assert.equal(published, false);
  assert.equal(turns, 8);
});
