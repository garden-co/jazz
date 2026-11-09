import { expect, it } from "vitest";
import { uploadApp, uploadFixture } from "./fixtures/encrypted-upload-fixture.js";

it("rejects both blocked uploads after rotation and recipient removal without replay", async () => {
  const fixture = await uploadFixture(false, false, true);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const pending: Promise<unknown>[] = [];
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
    const pulls = [0, 0];
    for (const [index, writer] of [fixture.owner, recipient.db].entries()) {
      let entered!: () => void;
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const upload = writer
        .insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "stale epoch",
          payload: (async function* () {
            pulls[index] = pulls[index]! + 1;
            entered();
            yield new Uint8Array([1]);
            await gate;
            yield new Uint8Array([2]);
          })(),
        })
        .then((write) => write.wait({ tier: "global" }));
      // Observe errors immediately while retaining each independent transaction.
      upload.catch(() => {});
      pending.push(upload);
      // Both writers must be eligible and pulling before the one accepted revocation.
      await started;
    }
    await fixture.owner.e2ee.spaces.revoke(uploadApp.projects, scope.id, recipient.id).wait();
    release();
    await Promise.all(pending.map((upload) => expect(upload).rejects.toThrow()));
    expect(pulls).toEqual([1, 1]);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "remote" })).toEqual([]);
  } finally {
    release();
    await Promise.allSettled(pending);
    await fixture.close();
  }
}, 60_000);
