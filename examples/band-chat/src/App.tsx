import { useState, type FormEvent } from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";
import { demoRoom } from "./fixture.js";
import "./app.css";

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const MAX_ATTACHMENT_BYTES = 256 * 1024;
const ALLOWED_ATTACHMENT_TYPES = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "text/plain",
  "application/pdf",
]);
function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return { appId, env: "dev", serverUrl, secret, ...overrides };
}

export function App({ config }: { config?: Partial<DbConfig> } = {}) {
  const auth = useLocalFirstAuth();
  if (config?.jwtToken) {
    return (
      <JazzProvider
        config={{ appId, env: "dev", serverUrl, ...config }}
        fallback={<p>Opening local stage…</p>}
      >
        <BandChat />
      </JazzProvider>
    );
  }
  const secret = config?.secret ?? auth.secret;
  if ((config?.secret === undefined && auth.isLoading) || !secret) return <p>Joining BandChat…</p>;
  return (
    <JazzProvider config={defaultConfig(secret, config)} fallback={<p>Opening local stage…</p>}>
      <BandChat />
    </JazzProvider>
  );
}

function BandChat() {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id;
  const { data: profiles = [] } = useAll(app.profiles.where({ userId: userId ?? "" }));
  const { data: rooms = [] } = useAll(app.rooms);
  const [roomId, setRoomId] = useState<string | null>(null);
  const selectedRoom = rooms.find((room) => room.id === roomId) ?? rooms[0];
  const { data: messages = [] } = useAll(
    selectedRoom
      ? app.messages.where({ roomId: selectedRoom.id })
      : app.messages.where({ roomId: "" }),
  );
  const [text, setText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [attachmentError, setAttachmentError] = useState<string | null>(null);

  const startDemo = async () => {
    if (!userId) return;
    const profile =
      profiles[0] ?? db.insert(app.profiles, { userId, displayName: "Local musician" }).value;
    const room = db.insert(app.rooms, { name: demoRoom.name, createdBy: userId }).value;
    db.insert(app.roomMembers, { roomId: room.id, userId });
    db.insert(app.messages, {
      roomId: room.id,
      senderId: profile.id,
      text: demoRoom.welcome,
      createdAt: new Date(),
    });
    setRoomId(room.id);
  };
  const send = async (event: FormEvent) => {
    event.preventDefault();
    if (!selectedRoom || !profiles[0] || (!text.trim() && !file)) return;
    if (file && (!ALLOWED_ATTACHMENT_TYPES.has(file.type) || file.size > MAX_ATTACHMENT_BYTES))
      return;
    const attachment = file ? new Uint8Array(await file.arrayBuffer()) : undefined;
    db.insert(app.messages, {
      roomId: selectedRoom.id,
      senderId: profiles[0].id,
      text: text.trim() || "Shared an attachment",
      attachment,
      attachmentName: file?.name,
      createdAt: new Date(),
    });
    setText("");
    setFile(null);
  };
  const chooseAttachment = (candidate: File | null) => {
    if (!candidate) {
      setFile(null);
      setAttachmentError(null);
      return;
    }
    if (!ALLOWED_ATTACHMENT_TYPES.has(candidate.type)) {
      setFile(null);
      setAttachmentError("Use PNG, JPEG, WebP, text, or PDF attachments.");
      return;
    }
    if (candidate.size > MAX_ATTACHMENT_BYTES) {
      setFile(null);
      setAttachmentError("Attachments are limited to 256 KB in this inline-bytes demo.");
      return;
    }
    setAttachmentError(null);
    setFile(candidate);
  };

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
      {!selectedRoom ? (
        <section className="empty">
          <h2>Start the soundcheck</h2>
          <p>Creates a deterministic synthetic room for this identity.</p>
          <button onClick={() => void startDemo()} disabled={!userId}>
            Open demo room
          </button>
        </section>
      ) : (
        <section className="chat">
          <aside>
            <h2>Rooms</h2>
            {rooms.map((room) => (
              <button
                className={room.id === selectedRoom.id ? "room active" : "room"}
                key={room.id}
                onClick={() => setRoomId(room.id)}
              >
                # {room.name}
              </button>
            ))}
          </aside>
          <div className="conversation">
            <h2># {selectedRoom.name}</h2>
            <ol aria-label="Messages">
              {messages.map((message) => (
                <li key={message.id}>
                  <strong>{message.senderId === profiles[0]?.id ? "You" : "Bandmate"}</strong>
                  <span>{message.text}</span>
                  {message.attachment && (
                    <small>
                      📎 {message.attachmentName ?? "attachment"} ({message.attachment.byteLength}{" "}
                      bytes)
                    </small>
                  )}
                </li>
              ))}
            </ol>
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
                  onChange={(event) => chooseAttachment(event.target.files?.[0] ?? null)}
                />
              </label>
              <button type="submit">Send locally</button>
            </form>
            {attachmentError && <p role="alert">{attachmentError}</p>}
          </div>
        </section>
      )}
    </main>
  );
}
