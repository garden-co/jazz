// A streamed attachment is sealed with the room's key: a member reads it back
// whole the way the room view does, and the server never holds its bytes.
import { afterEach, describe, expect, it } from "vitest";
import { app, cleanUp, createRoom, device, memberRow, serve, storedBytes, until } from "./harness";

afterEach(cleanUp);

describe("attachment bytes", () => {
  it.each(["its first encrypted write", "after a text message"])(
    "streams as a room's %s and a member reads the same bytes back",
    async (when) => {
      const host = await serve();
      const alice = await device(host);
      const bob = await device(host);
      const roomId = await createRoom(alice, "Rehearsal");
      await alice.db
        .insert(app.__e2ee_members, await memberRow(alice, roomId, bob.account))
        .wait({ tier: "global" });
      if (when === "after a text message")
        await alice.db
          .insert(app.messages, { roomId, senderId: alice.profile.id, text: "hi" })
          .wait({ tier: "global" });
      const bytes = new Uint8Array(300_000).map((_, index) => index % 251);
      const sent = await alice.db.insertStreaming(app.messages, {
        roomId,
        senderId: alice.profile.id,
        text: "",
        attachmentName: "take.bin",
        attachmentType: "application/octet-stream",
        attachmentSize: bytes.length,
        attachment: new Blob([bytes]).stream(),
      });
      await sent.wait({ tier: "global" });
      await alice.db.encryption.keys.deliver();

      // The room view's read: the room's id beside the bytes.
      const read = await until(async () => {
        const rows = await bob.db.all(
          app.messages.where({ id: sent.value.id }).select("roomId", "attachment"),
          { tier: "remote" },
        );
        return rows[0]?.attachment ? rows[0] : undefined;
      }, "bob to read the attachment");
      expect(Buffer.from(read.attachment!).equals(Buffer.from(bytes))).toBe(true);
      expect((await storedBytes(host.dir)).includes(Buffer.from(bytes.subarray(1000, 1100)))).toBe(
        false,
      );
    },
    120_000,
  );
});
