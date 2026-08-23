import { useState, type FormEvent } from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";
import { provisionDemo } from "./provisioning.js";
import "./app.css";

const appId = import.meta.env.VITE_JAZZ_APP_ID,
  serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const MAX_ATTACHMENT_BYTES = 256 * 1024;
const allowedTypes = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "text/plain",
  "application/pdf",
]);
export function App({ config }: { config?: Partial<DbConfig> } = {}) {
  const auth = useLocalFirstAuth();
  const providerConfig = config?.jwtToken
    ? { appId, env: "dev", serverUrl, ...config }
    : auth.secret || config?.secret
      ? { appId, env: "dev", serverUrl, secret: config?.secret ?? auth.secret!, ...config }
      : null;
  if (!providerConfig) return <p>Joining BandChat…</p>;
  return (
    <JazzProvider config={providerConfig} fallback={<p>Opening local stage…</p>}>
      <SessionShell />
    </JazzProvider>
  );
}
function SessionShell() {
  const session = useSession();
  return (
    <main className="shell">
      <header>
        <span className="eyebrow">LOCAL-FIRST BAND HQ</span>
        <h1>BandChat</h1>
        <p>
          Rooms are visible and writable only to members. Messages save locally before Jazz
          reconnects them.
        </p>
      </header>
      {session?.user_id ? (
        <RoomWorkspace userId={session.user_id} />
      ) : (
        <section className="empty">
          <h2>Identity unavailable</h2>
          <p>Waiting for an authenticated Jazz session.</p>
        </section>
      )}
    </main>
  );
}
function RoomWorkspace({ userId }: { userId: string }) {
  const { data: rooms = [] } = useAll(app.rooms);
  const { data: profiles = [] } = useAll(app.profiles.where({ userId }));
  const db = useDb();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected = rooms.find((room) => room.id === selectedId) ?? rooms[0];
  const provision = () =>
    setSelectedId(provisionDemo(db, userId, { profileId: profiles[0]?.id, roomId: selected?.id }));
  if (!selected)
    return (
      <section className="empty">
        <h2>Start the soundcheck</h2>
        <p>Creates a deterministic synthetic room for this identity.</p>
        <button onClick={provision}>Open demo room</button>
      </section>
    );
  return (
    <section className="chat">
      <RoomList selectedId={selected.id} onSelect={setSelectedId} />
      <Conversation roomId={selected.id} userId={userId} />
    </section>
  );
}
function RoomList({
  selectedId,
  onSelect,
}: {
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  const { data: rooms = [] } = useAll(app.rooms);
  return (
    <aside>
      <h2>Rooms</h2>
      {rooms.map((room) => (
        <button
          className={room.id === selectedId ? "room active" : "room"}
          key={room.id}
          onClick={() => onSelect(room.id)}
        >
          # {room.name}
        </button>
      ))}
    </aside>
  );
}
function Conversation({ roomId, userId }: { roomId: string; userId: string }) {
  const { data: rooms = [] } = useAll(app.rooms.where({ id: roomId }));
  const { data: messages = [] } = useAll(app.messages.where({ roomId }));
  const room = rooms[0];
  return (
    <div className="conversation">
      {room ? (
        <>
          <h2># {room.name}</h2>
          <MessageList messages={messages} userId={userId} />
          <Composer roomId={roomId} userId={userId} />
        </>
      ) : (
        <p>Loading room…</p>
      )}
    </div>
  );
}
function MessageList({
  messages,
  userId,
}: {
  messages: Array<{
    id: string;
    senderId: string;
    text: string;
    attachment?: Uint8Array;
    attachmentName?: string;
  }>;
  userId: string;
}) {
  const { data: profiles = [] } = useAll(app.profiles.where({ userId }));
  return (
    <ol aria-label="Messages">
      {messages.map((message) => (
        <li key={message.id}>
          <strong>{message.senderId === profiles[0]?.id ? "You" : "Bandmate"}</strong>
          <span>{message.text}</span>
          {message.attachment && (
            <small>
              📎 {message.attachmentName ?? "attachment"} ({message.attachment.byteLength} bytes)
            </small>
          )}
        </li>
      ))}
    </ol>
  );
}
function Composer({ roomId, userId }: { roomId: string; userId: string }) {
  const db = useDb();
  const { data: profiles = [] } = useAll(app.profiles.where({ userId }));
  const [text, setText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const choose = (candidate: File | null) => {
    if (!candidate) return setFile(null);
    if (!allowedTypes.has(candidate.type)) {
      setFile(null);
      return setError("Use PNG, JPEG, WebP, text, or PDF attachments.");
    }
    if (candidate.size > MAX_ATTACHMENT_BYTES) {
      setFile(null);
      return setError("Attachments are limited to 256 KB in this inline-bytes demo.");
    }
    setError(null);
    setFile(candidate);
  };
  const send = async (event: FormEvent) => {
    event.preventDefault();
    if (!profiles[0] || (!text.trim() && !file)) return;
    const attachment = file ? new Uint8Array(await file.arrayBuffer()) : undefined;
    db.insert(app.messages, {
      roomId,
      senderId: profiles[0].id,
      text: text.trim() || "Shared an attachment",
      attachment,
      attachmentName: file?.name,
      createdAt: new Date(),
    });
    setText("");
    setFile(null);
  };
  return (
    <>
      <form onSubmit={(event) => void send(event)}>
        <input
          aria-label="Message"
          value={text}
          onChange={(event) => setText(event.target.value)}
          placeholder="Write a message"
        />
        <label className="file">
          Attach
          <input
            aria-label="Attachment"
            type="file"
            onChange={(event) => choose(event.target.files?.[0] ?? null)}
          />
        </label>
        <button type="submit">Send locally</button>
      </form>
      {error && <p role="alert">{error}</p>}
    </>
  );
}
