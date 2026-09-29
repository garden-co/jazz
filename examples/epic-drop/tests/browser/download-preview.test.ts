import { afterEach, describe, expect, it } from "vitest";
import { createAccountManager, createDb, generateAuthSecret, type Db } from "jazz-tools";
import { prepareTestAccount } from "../../../testing/accounts.js";
import { app } from "../../schema.js";
import { hexDump, readFileBlob, readFileRange } from "../../src/large-values.js";
import { createInvite, inviteLink, parseInviteHash, redeemInvite } from "../../src/sharing.js";
import { APP_ID, TEST_PORT } from "./test-constants.js";

const dbs: Db[] = [];
const serverUrl = `http://127.0.0.1:${TEST_PORT}`;

afterEach(async () => {
  await Promise.all(dbs.splice(0).map((db) => db.shutdown()));
});

async function openRemoteDb(label: string): Promise<Db> {
  const db = await createDb({
    appId: APP_ID,
    serverUrl,
    account: await prepareTestAccount(
      createAccountManager,
      APP_ID,
      serverUrl,
      generateAuthSecret(),
    ),
    driver: { type: "persistent", dbName: `epic-drop-${label}-${crypto.randomUUID()}` },
  });
  dbs.push(db);
  return db;
}

function sessionUser(db: Db): string {
  const id = db.getAuthState().session?.user.account;
  if (!id) throw new Error("expected local-first user session");
  return id;
}

/** A text file whose bytes are predictable at every offset. */
function pattern(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) bytes[i] = 0x61 + (i % 26);
  return bytes;
}

async function* chunks(bytes: Uint8Array, size: number) {
  for (let offset = 0; offset < bytes.length; offset += size)
    yield bytes.slice(offset, offset + size);
}

describe("EpicDrop download and preview", () => {
  it("downloads a whole value and previews text one range at a time", async () => {
    const db = await openRemoteDb("preview");
    const userId = sessionUser(db);
    const folder = db.insert(app.folders, { name: "Notes", owner_id: userId });
    const bytes = pattern(200 * 1024);
    const written = await db.insertStreaming(app.files, {
      folder_id: folder.value.id,
      name: "alphabet.txt",
      content_type: "text/plain",
      size_bytes: bytes.length,
      owner_id: userId,
      contents: chunks(bytes, 48 * 1024),
    });
    const file = { id: written.value.id, size_bytes: bytes.length };

    const blob = await readFileBlob(db, file.id, "text/plain");
    expect(blob.type).toBe("text/plain");
    expect(new Uint8Array(await blob.arrayBuffer())).toEqual(bytes);

    const first = await readFileRange(db, file, 0, 64 * 1024);
    expect(first).toEqual(bytes.subarray(0, 64 * 1024));
    const middle = await readFileRange(db, file, 100_000, 100_016);
    expect(new TextDecoder().decode(middle)).toBe(
      new TextDecoder().decode(bytes.subarray(100_000, 100_016)),
    );
    // Ranges past the end are clamped to the file, as the preview's "Show more" relies on.
    const tail = await readFileRange(db, file, bytes.length - 10, bytes.length + 64 * 1024);
    expect(tail).toEqual(bytes.subarray(bytes.length - 10));
    expect(await readFileRange(db, file, bytes.length, bytes.length + 1)).toHaveLength(0);

    expect(hexDump(new Uint8Array([0x25, 0x50, 0x44, 0x46, 0x2d]))).toBe(
      "00000000  25 50 44 46 2d" + " ".repeat(47 - 14) + "  %PDF-",
    );
  });

  it("shares a folder through an invite link and serves ranges to the member", async () => {
    const alice = await openRemoteDb("alice");
    const bob = await openRemoteDb("bob");
    const aliceId = sessionUser(alice);
    const bobId = sessionUser(bob);

    const demos = alice.insert(app.folders, { name: "Demos", owner_id: aliceId });
    await demos.wait({ tier: "global" });
    const bytes = pattern(8 * 1024);
    const upload = await alice.insertStreaming(app.files, {
      folder_id: demos.value.id,
      name: "lyrics.txt",
      content_type: "text/plain",
      size_bytes: bytes.length,
      owner_id: aliceId,
      contents: chunks(bytes, 4096),
    });
    await upload.wait({ tier: "global" });

    const invite = createInvite(alice, demos.value.id, "viewer");
    await alice.all(app.folderInvites, { tier: "global" });
    const parsed = parseInviteHash(new URL(inviteLink(invite, "https://drop.test/")).hash);
    expect(parsed).toEqual(invite);

    // Before joining, Bob sees nothing.
    await expect(bob.all(app.folders, { tier: "global" })).resolves.toEqual([]);

    // A forged code is rejected by the server.
    await expect(
      redeemInvite(bob, { ...invite, code: crypto.randomUUID() }, bobId),
    ).rejects.toThrow();

    await redeemInvite(bob, parsed!, bobId);
    const folders = await bob.all(app.folders, { tier: "global" });
    expect(folders.map((folder) => folder.name)).toEqual(["Demos"]);

    const [listed] = await bob.all(
      app.files.where({ folder_id: demos.value.id }).select("id", "name", "size_bytes"),
      { tier: "global" },
    );
    expect(listed).toMatchObject({ name: "lyrics.txt", size_bytes: bytes.length });
    const [page] = await bob.all(
      app.files.where({ id: listed!.id }).select({ contents: { from: 4090, to: 4100 } }),
      { tier: "global" },
    );
    expect(page!.contents).toEqual(bytes.subarray(4090, 4100));

    // Viewers cannot add files.
    await expect(
      bob
        .insertStreaming(app.files, {
          folder_id: demos.value.id,
          name: "nope.txt",
          content_type: "text/plain",
          size_bytes: 1,
          owner_id: bobId,
          contents: chunks(new Uint8Array([1]), 1),
        })
        .then((write) => write.wait({ tier: "global" })),
    ).rejects.toThrow();
  });
});
