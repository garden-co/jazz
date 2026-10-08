import { expect, it } from "vitest";
import { uploadApp, uploadFixture, uploadBytes } from "./fixtures/encrypted-upload-fixture.js";

it("keeps both existing-space keys borrowed until their staged files reach the granted recipient", async () => {
  const fixture = await uploadFixture(false, false, true);
  try {
    const recipient = await fixture.getRecipient();
    const first = await fixture.owner
      .insert(uploadApp.projects, { title: "First space" })
      .wait({ tier: "global" });
    const second = await fixture.owner
      .insert(uploadApp.projects, { title: "Second space" })
      .wait({ tier: "global" });
    await fixture.owner.e2ee.spaces.grant(uploadApp.projects, first.id, recipient.id).wait();
    await fixture.owner.e2ee.spaces.grant(uploadApp.projects, second.id, recipient.id).wait();
    const firstBytes = Uint8Array.from({ length: 131_091 }, (_, index) => index % 251);
    const secondBytes = Uint8Array.from({ length: 131_093 }, (_, index) => 255 - (index % 251));
    const result = await fixture.owner.streamingTransaction((plan) => {
      const firstFile = plan.insertStreaming(uploadApp.files, {
        projectId: first.id,
        name: "first.png",
        payload: uploadBytes(firstBytes),
      });
      const secondFile = plan.insertStreaming(uploadApp.files, {
        projectId: second.id,
        name: "second.png",
        payload: uploadBytes(secondBytes),
      });
      return { first: firstFile.id, second: secondFile.id };
    });
    const ids = await result.wait({ tier: "global" });
    expect(
      await recipient.db.one(uploadApp.files.where({ id: ids.first }), { tier: "global" }),
    ).toEqual({
      id: ids.first,
      projectId: first.id,
      name: "first.png",
      payload: firstBytes,
    });
    expect(
      await recipient.db.one(uploadApp.files.where({ id: ids.second }), { tier: "global" }),
    ).toEqual({
      id: ids.second,
      projectId: second.id,
      name: "second.png",
      payload: secondBytes,
    });
  } finally {
    await fixture.close();
  }
}, 60_000);
