import { expect, it } from "vitest";
import { uploadApp, uploadFixture } from "./fixtures/encrypted-upload-fixture.js";

it("preserves ordinary streamed updates to an existing scope", async () => {
  const fixture = await uploadFixture(false, false, false, true);
  try {
    const row = fixture.owner.insert(uploadApp.projects, { title: "Before" });
    await row.wait({ tier: "global" });
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield "After";
    })();
    const update = await fixture.owner.updateStreaming(uploadApp.projects, row.value.id, {
      title: source,
    });
    await update.wait({ tier: "global" });
    expect(consumed).toBe(1);
    expect(
      await fixture.owner.one(uploadApp.projects.where({ id: row.value.id }), { tier: "remote" }),
    ).toEqual({ ...row.value, title: "After" });
  } finally {
    await fixture.close();
  }
}, 30_000);

it("raw transaction streaming refuses encrypted payloads before pulling plaintext", async () => {
  const fixture = await uploadFixture();
  let tx: ReturnType<typeof fixture.owner.beginSerializableTransaction> | undefined;
  try {
    const scope = fixture.owner.insert(uploadApp.projects, { title: "Existing" });
    await scope.wait({ tier: "global" });
    const row = fixture.owner.insert(uploadApp.files, {
      projectId: scope.value.id,
      payload: new Uint8Array([9]),
      name: "Existing",
    });
    await row.wait({ tier: "global" });
    let consumed = 0;
    const source = (async function* () {
      consumed++;
      yield new Uint8Array([1, 2, 3]);
    })();
    tx = fixture.owner.beginSerializableTransaction();
    await expect(
      tx.updateStreaming(uploadApp.files, row.value.id, { payload: source }),
    ).rejects.toThrow("Encrypted streaming is not supported");
    expect(consumed).toBe(0);
    await tx.rollback();
    tx = undefined;
    const unchanged = await fixture.owner.one(uploadApp.files.where({ id: row.value.id }), {
      tier: "remote",
    });
    expect(unchanged?.payload).toEqual(new Uint8Array([9]));
  } finally {
    await tx?.rollback();
    await fixture.close();
  }
}, 30_000);
