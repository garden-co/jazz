import type { Db } from "jazz-tools";
import { describe, expect, it, vi } from "vitest";
import { app } from "../schema";
import { canInsertChatMessage } from "./chat-permissions";

describe("canInsertChatMessage", () => {
  it("asks Db.canInsert with the message table and treats only true as writable", async () => {
    const message = {
      author_name: "alice",
      chat_id: "chat-01",
      text: "Hello",
      sent_at: new Date("2026-04-30T00:00:00.000Z"),
    };
    const canInsert = vi.fn<Db["canInsert"]>().mockResolvedValue("unknown");
    const db = { canInsert } as Pick<Db, "canInsert">;

    await expect(canInsertChatMessage(db, app.messages, message)).resolves.toBe(false);
    expect(canInsert).toHaveBeenCalledWith(app.messages, message);

    canInsert.mockResolvedValueOnce(true);
    await expect(canInsertChatMessage(db, app.messages, message)).resolves.toBe(true);
  });
});
