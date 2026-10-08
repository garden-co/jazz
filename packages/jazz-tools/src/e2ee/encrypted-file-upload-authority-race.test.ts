import { expect, it } from "vitest";
import { uploadApp, uploadFixture } from "./fixtures/encrypted-upload-fixture.js";

it.each(["rotation", "removal"] as const)(
  "rejects %s during a blocked source without replay or an accepted file",
  async (change) => {
    const fixture = await uploadFixture(false, false, true);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const started = new Promise<void>((resolve) => {
      entered = resolve;
    });
    try {
      const recipient = await fixture.getRecipient();
      const scope = await fixture.owner
        .insert(
          uploadApp.projects,
          { title: "Shared" },
          {
            initialRecipients: [fixture.ownerId, recipient.id],
          },
        )
        .wait({ tier: "global" });
      let pulls = 0;
      const writer = change === "rotation" ? fixture.owner : recipient.db;
      const pending = writer
        .insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "stale epoch",
          payload: (async function* () {
            pulls++;
            entered();
            yield new Uint8Array([1]);
            await gate;
            yield new Uint8Array([2]);
          })(),
        })
        .then((write) => write.wait({ tier: "global" }));
      const rejected = expect(pending).rejects.toThrow();
      await started;
      await fixture.owner.e2ee.spaces.revoke(uploadApp.projects, scope.id, recipient.id).wait();
      release();
      await rejected;
      expect(pulls).toBe(1);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
    } finally {
      release();
      await fixture.close();
    }
  },
  60_000,
);
