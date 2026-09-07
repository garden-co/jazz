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
  let time = 0;
  const published = await waitForPublication(
    () => turns === 3,
    async () => {
      turns += 1;
      time += 10;
    },
    () => time,
  );
  assert.equal(published, true);
  assert.equal(turns, 3);
});

test("publication wait rejects an empty opening until the run marker arrives", async () => {
  let marker = false;
  let time = 0;
  const published = await waitForPublication(
    () => marker,
    async () => {
      time += 10;
      if (time === 30) marker = true;
    },
    () => time,
  );
  assert.equal(published, true);
  assert.equal(time, 30);
});

test("publication wait fails at its elapsed-time deadline", async () => {
  let time = 0;
  const published = await waitForPublication(
    () => false,
    async () => {
      time += 10_000;
    },
    () => time,
  );
  assert.equal(published, false);
  assert.equal(time, 30_000);
});
