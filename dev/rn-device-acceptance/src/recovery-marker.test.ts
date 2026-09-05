import assert from "node:assert/strict";
import test from "node:test";
import { requireCoreRecoveryMarker } from "./recovery-marker.ts";

const title = "high-level-foreground-row:12345678-1234-4234-9234-123456789abc";

test("recovery acknowledgement rejects a missing subscription marker without reading local data", async () => {
  let reads = 0;
  await assert.rejects(
    requireCoreRecoveryMarker(
      () => false,
      async () => {
        reads++;
        return [`${title}:recovered-by-core`];
      },
      title,
      async () => false,
    ),
    /original installed subscription did not receive Core's post-recovery marker/,
  );
  assert.equal(reads, 0);
});

test("recovery acknowledgement rejects a missing Core marker after subscription publication", async () => {
  await assert.rejects(
    requireCoreRecoveryMarker(
      () => true,
      async () => [title],
      title,
      async (observed) => observed(),
    ),
    /original installed foreground did not read Core's post-recovery marker/,
  );
});

test("recovery acknowledgement accepts the exact Core marker after publication", async () => {
  await requireCoreRecoveryMarker(
    () => true,
    async () => [title, `${title}:recovered-by-core`],
    title,
    async (observed) => observed(),
  );
});
