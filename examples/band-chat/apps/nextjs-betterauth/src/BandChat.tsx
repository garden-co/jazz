"use client";

import { useState, type FormEvent } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";

// This only bounds files selected through this component. `s.bytes()` has no
// corresponding schema or policy size constraint, so it must not be treated as
// an authorization or security boundary for direct database writes.
const ATTACHMENT_PICKER_MAX_BYTES = 256 * 1024;
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
  return session?.user ? <RoomWorkspace author={session.user} /> : <p>Loading identity…</p>;
}

/** Browser receipt entrypoint. The production dashboard never uses local-first auth here. */
export function BandChatPreview({ config }: { config: DbConfig }) {
  return (
    <JazzProvider config={config} fallback={<p>Opening local stage…</p>}>
      <BandChat />
    </JazzProvider>
  );
}

function RoomWorkspace({ author }: { author: string }) {
  const db = useDb();
  const { data: rooms = [] } = useAll(app.rooms.select("*", "$createdBy").orderBy("name", "asc"));
  const { data: profiles = [] } = useAll(app.profiles.where({ author }));
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
            author,
            displayName: "Band member",
          })
        ).value;
      const room = (await db.insert(app.rooms, { name })).value;
      await db.insert(app.roomMembers, { roomId: room.id, memberAuthor: author });
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
          <Conversation
            canEditMembership={selectedRoom.$createdBy === author}
            roomId={selectedRoom.id}
            author={author}
          />
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

function Conversation({
  canEditMembership,
  roomId,
  author,
}: {
  canEditMembership: boolean;
  roomId: string;
  author: string;
}) {
  const { data: rooms = [] } = useAll(app.rooms.where({ id: roomId }));
  const { data: messages = [] } = useAll(
    app.messages.where({ roomId }).select("*", "$createdAt").orderBy("$createdAt", "asc"),
  );
  const { data: profiles = [] } = useAll(app.profiles.where({ author }));
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
      {canEditMembership ? <MembershipEditor roomId={roomId} /> : null}
      <Composer roomId={roomId} profileId={profiles[0]?.id ?? null} />
    </section>
  );
}

function MembershipEditor({ roomId }: { roomId: string }) {
  const db = useDb();
  const { data: memberships = [] } = useAll(app.roomMembers.where({ roomId }));
  const [memberAuthor, setMemberAuthor] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function invite(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const invitedAuthor = memberAuthor.trim();
    if (!invitedAuthor || memberships.some((member) => member.memberAuthor === invitedAuthor))
      return;
    setError(null);
    try {
      await db.insert(app.roomMembers, { roomId, memberAuthor: invitedAuthor });
      setMemberAuthor("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  async function remove(membershipId: string) {
    setError(null);
    try {
      await db.delete(app.roomMembers, membershipId);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    }
  }

  return (
    <section aria-label="Room membership">
      <h3>Members</h3>
      <ul>
        {memberships.map((membership) => (
          <li key={membership.id}>
            <span>
              <small>{membership.memberAuthor}</small>
            </span>
            <button onClick={() => void remove(membership.id)} type="button">
              Remove
            </button>
          </li>
        ))}
      </ul>
      <form onSubmit={(event) => void invite(event)}>
        <label>
          Invite canonical author
          <input
            aria-label="Invite canonical author"
            onChange={(event) => setMemberAuthor(event.target.value)}
            value={memberAuthor}
          />
        </label>
        <button type="submit">Invite member</button>
      </form>
      {error ? <p role="alert">{error}</p> : null}
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
    if (candidate.size > ATTACHMENT_PICKER_MAX_BYTES) {
      setFile(null);
      setError(
        "The attachment picker accepts files up to 256 KiB; this is client-side validation only.",
      );
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
