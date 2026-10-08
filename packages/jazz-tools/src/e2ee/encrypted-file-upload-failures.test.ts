import { expect, it } from "vitest";
import {
  uploadApp,
  legacyUploadApp,
  uploadFixture,
  uploadBytes,
} from "./fixtures/encrypted-upload-fixture.js";

it.each(["source-failure", "denied"] as const)(
  "publishes neither new scope nor controls on %s",
  async (failure) => {
    const fixture = await uploadFixture(false, failure === "denied");
    try {
      let pulls = 0;
      const pending = fixture.owner
        .streamingTransaction((plan) => {
          const scope = plan.insert(uploadApp.projects, { title: "Must remain absent" });
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "first sibling",
            payload: uploadBytes(new Uint8Array([1, 2])),
          });
          return plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "second sibling",
            payload: (async function* () {
              pulls++;
              yield new Uint8Array([3]);
              if (failure === "source-failure") throw new Error("fixture source failed");
            })(),
          });
        })
        .then((write) => write.wait({ tier: "global" }));
      await expect(pending).rejects.toThrow(
        failure === "source-failure" ? "fixture source failed" : undefined,
      );
      expect(pulls).toBe(1);
      expect(await fixture.owner.all(uploadApp.projects, { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.__e2ee_spaces, { tier: "global" })).toEqual([]);
      expect(await fixture.owner.all(uploadApp.__e2ee_space_grants, { tier: "global" })).toEqual(
        [],
      );
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it("rejects keyless writers and every invalid declaration before pulling any source", async () => {
  const fixture = await uploadFixture(false, false, true);
  try {
    const recipient = await fixture.getRecipient();
    const scope = await fixture.owner
      .insert(uploadApp.projects, { title: "Private" })
      .wait({ tier: "global" });
    let pulls = 0;
    const source = () =>
      (async function* () {
        pulls++;
        yield new Uint8Array([1]);
      })();
    await expect(
      recipient.db.insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "not granted",
        payload: source(),
      }),
    ).rejects.toThrow();
    await expect(
      fixture.owner.streamingTransaction((plan) => {
        plan.insertStreaming(uploadApp.files, {
          projectId: scope.id,
          name: "valid first",
          payload: source(),
        });
        plan.updateStreaming(uploadApp.files, "invalid UUID", { payload: source() });
      }),
    ).rejects.toThrow();
    await expect(
      fixture.owner.updateStreaming(
        uploadApp.files,
        crypto.randomUUID(),
        {
          payload: source(),
        },
        { branch: { projectId: scope.id } },
      ),
    ).rejects.toThrow("root-view");
    expect(pulls).toBe(0);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it.each(["resolved", "pending"] as const)(
  "rejects an invalid byte source despite %s cancellation and leaves staged siblings unpublished",
  async (cancellation) => {
    const fixture = await uploadFixture();
    try {
      const scope = await fixture.owner
        .insert(uploadApp.projects, { title: "Cancellation" })
        .wait({ tier: "global" });
      let cancelled = false;
      const source = new ReadableStream<Uint8Array | string>(
        {
          pull(controller) {
            controller.enqueue("not bytes");
          },
          cancel() {
            cancelled = true;
            if (cancellation === "pending") return new Promise<void>(() => {});
          },
        },
        { highWaterMark: 0 },
      );
      await expect(
        fixture.owner.streamingTransaction((plan) => {
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "sibling",
            payload: uploadBytes(new Uint8Array([1])),
          });
          plan.insertStreaming(uploadApp.files, {
            projectId: scope.id,
            name: "invalid",
            payload: source,
          });
        }),
      ).rejects.toThrow();
      expect(cancelled).toBe(true);
      expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
      const replacement = await fixture.owner.insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "subsequent upload",
        payload: uploadBytes(new Uint8Array([8, 9])),
      });
      await replacement.wait({ tier: "global" });
      expect(
        await fixture.owner.one(uploadApp.files.where({ id: replacement.value.id }), {
          tier: "global",
        }),
      ).toEqual({
        id: replacement.value.id,
        projectId: scope.id,
        name: "subsequent upload",
        payload: new Uint8Array([8, 9]),
      });
    } finally {
      await fixture.close();
    }
  },
  60_000,
);

it("rejects a later keyless space before pulling an earlier eligible source", async () => {
  const fixture = await uploadFixture(false, false, true);
  try {
    const recipient = await fixture.getRecipient();
    const shared = await fixture.owner
      .insert(
        uploadApp.projects,
        { title: "Shared" },
        {
          initialRecipients: [fixture.ownerId, recipient.id],
        },
      )
      .wait({ tier: "global" });
    const privateScope = await fixture.owner
      .insert(uploadApp.projects, { title: "Private" })
      .wait({ tier: "global" });
    let pulls = 0;
    const source = () =>
      (async function* () {
        pulls++;
        yield new Uint8Array([1]);
      })();
    await expect(
      recipient.db.streamingTransaction((plan) => {
        plan.insertStreaming(uploadApp.files, {
          projectId: shared.id,
          name: "eligible",
          payload: source(),
        });
        plan.insertStreaming(uploadApp.files, {
          projectId: privateScope.id,
          name: "keyless",
          payload: source(),
        });
      }),
    ).rejects.toThrow();
    expect(pulls).toBe(0);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);

it("keeps a legacy scope but rejects its first file and initial controls together on permission denial", async () => {
  const fixture = await uploadFixture(false, true);
  try {
    const legacy = await fixture.getLegacy();
    const scope = await legacy
      .insert(legacyUploadApp.projects, { title: "Legacy remains" })
      .wait({ tier: "global" });
    const pending = fixture.owner
      .insertStreaming(uploadApp.files, {
        projectId: scope.id,
        name: "denied first file",
        payload: uploadBytes(new Uint8Array([1, 2, 3])),
      })
      .then((write) => write.wait({ tier: "global" }));
    await expect(pending).rejects.toThrow();
    expect(await fixture.owner.all(uploadApp.projects, { tier: "global" })).toEqual([scope]);
    expect(await fixture.owner.all(uploadApp.files.select("id"), { tier: "global" })).toEqual([]);
    expect(await fixture.owner.all(uploadApp.__e2ee_spaces, { tier: "global" })).toEqual([]);
    expect(await fixture.owner.all(uploadApp.__e2ee_space_grants, { tier: "global" })).toEqual([]);
  } finally {
    await fixture.close();
  }
}, 60_000);
