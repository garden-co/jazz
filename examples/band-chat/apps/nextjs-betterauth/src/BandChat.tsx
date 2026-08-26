"use client";

import { useState, type FormEvent } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";

const MAX_ATTACHMENT_BYTES = 256 * 1024;
const allowedAttachmentTypes = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "text/plain",
  "application/pdf",
]);

/** Rendered inside the external-auth provider in the Next dashboard. */
export function BandChat() {
  const session = useSession();
  return session?.user_id ? <RoomWorkspace userId={session.user_id} /> : <p>Loading identity…</p>;
}

/** Browser receipt entrypoint. The production dashboard never uses local-first auth here. */
export function BandChatPreview({ config }: { config: DbConfig }) {
  return (
    <JazzProvider config={config} fallback={<p>Opening local stage…</p>}>
      <BandChat />
    </JazzProvider>
  );
}

function RoomWorkspace({ userId }: { userId: string }) {
  const db = useDb();
  const { data: rooms = [] } = useAll(app.rooms.orderBy("name", "asc"));
  const { data: profiles = [] } = useAll(app.profiles.where({ userId }));
  const [selectedRoomId, setSelectedRoomId] = useState<string | null>(null);
  const [newRoomName, setNewRoomName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const selectedRoom = rooms.find((room) => room.id === selectedRoomId) ?? rooms[0];

  async function createRoom(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const name = newRoomName.trim();
    if (!name) return;
    setError(null);
    try {
      // This is an explicit user action. It is intentionally not hidden in a
      // query hook, so reopening a read-only view cannot provision a room.
      const profile =
        profiles[0] ??
        (
          await db.insert(app.profiles, {
            userId,
            displayName: "Band member",
          })
        ).value;
      const room = (await db.insert(app.rooms, { name })).value;
      await db.insert(app.roomMembers, { roomId: room.id, userId });
      // Keep the returned profile live so the write order is clear to readers.
      void profile;
      setSelectedRoomId(room.id);
      setNewRoomName("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <main className="shell">
      <header>
        <span className="eyebrow">LOCAL-FIRST BAND HQ</span>
        <h1>BandChat</h1>
        <p>Room creators admit members; member messages are stored locally before reconnecting.</p>
      </header>
      <section className="workspace">
        <aside>
          <h2>Rooms</h2>
          <form onSubmit={(event) => void createRoom(event)}>
            <label htmlFor="room-name">New room</label>
            <input
              id="room-name"
              value={newRoomName}
              onChange={(event) => setNewRoomName(event.target.value)}
              placeholder="Rehearsal"
            />
            <button type="submit">Create room</button>
          </form>
          {rooms.map((room) => (
            <button
              className={room.id === selectedRoom?.id ? "room active" : "room"}
              key={room.id}
              onClick={() => setSelectedRoomId(room.id)}
              type="button"
            >
              # {room.name}
            </button>
          ))}
        </aside>
        {selectedRoom ? (
          <Conversation roomId={selectedRoom.id} userId={userId} />
        ) : (
          <section className="empty">
            <h2>Start the soundcheck</h2>
            <p>
              Create a room to add your own membership and begin an offline-capable conversation.
            </p>
          </section>
        )}
      </section>
      {error ? <p role="alert">{error}</p> : null}
    </main>
  );
}

function Conversation({ roomId, userId }: { roomId: string; userId: string }) {
  const { data: rooms = [] } = useAll(app.rooms.where({ id: roomId }));
  const { data: messages = [] } = useAll(
    app.messages.where({ roomId }).select("*", "$createdAt").orderBy("$createdAt", "asc"),
  );
  const { data: profiles = [] } = useAll(app.profiles.where({ userId }));
  const room = rooms[0];
  if (!room) return <p>Loading room…</p>;

  return (
    <section className="conversation">
      <h2># {room.name}</h2>
      <ol aria-label="Messages">
        {messages.map((message) => (
          <li key={message.id}>
            <strong>{message.senderId === profiles[0]?.id ? "You" : "Bandmate"}</strong>
            <span>{message.text}</span>
            {message.attachment ? (
              <small>
                📎 {message.attachmentName ?? "attachment"} ({message.attachment.byteLength} bytes)
              </small>
            ) : null}
          </li>
        ))}
      </ol>
      <Composer roomId={roomId} profileId={profiles[0]?.id ?? null} />
    </section>
  );
}

function Composer({ roomId, profileId }: { roomId: string; profileId: string | null }) {
  const db = useDb();
  const [text, setText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);

  function chooseFile(candidate: File | null) {
    if (!candidate) return setFile(null);
    if (!allowedAttachmentTypes.has(candidate.type)) {
      setFile(null);
      setError("Use PNG, JPEG, WebP, text, or PDF attachments.");
      return;
    }
    if (candidate.size > MAX_ATTACHMENT_BYTES) {
      setFile(null);
      setError("Attachments are limited to 256 KB in this inline-bytes example.");
      return;
    }
    setError(null);
    setFile(candidate);
  }

  async function send(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!profileId || (!text.trim() && !file)) return;
    const attachment = file ? new Uint8Array(await file.arrayBuffer()) : undefined;
    await db.insert(app.messages, {
      roomId,
      senderId: profileId,
      text: text.trim() || "Shared an attachment",
      attachment,
      attachmentName: file?.name,
    });
    setText("");
    setFile(null);
  }

  return (
    <>
      <form onSubmit={(event) => void send(event)}>
        <input
          aria-label="Message"
          value={text}
          onChange={(event) => setText(event.target.value)}
          placeholder="Write a message"
        />
        <label>
          Attach
          <input
            aria-label="Attachment"
            type="file"
            onChange={(event) => chooseFile(event.target.files?.[0] ?? null)}
          />
        </label>
        <button type="submit">Send locally</button>
      </form>
      {error ? <p role="alert">{error}</p> : null}
    </>
  );
}
