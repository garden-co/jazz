import { useState } from "react";
import { useAll, useDb, useOne, useSession } from "jazz-tools/react";
import { app } from "../schema.js";

export function App() {
  const db = useDb();
  const accountId = useSession()?.user.account;
  const [chatId, setChatId] = useState(() => new URLSearchParams(location.search).get("chat"));
  const [text, setText] = useState("");
  const [recipient, setRecipient] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const chat = useOne(chatId ? app.chats.where({ id: chatId }) : undefined);
  const messages = useAll(
    chatId ? app.messages.where({ chatId }).orderBy("$createdAt", "asc") : undefined,
  );

  async function run(action: () => Promise<void>) {
    if (busy) return;
    setBusy(true);
    setError("");
    setStatus("");
    try {
      await action();
    } catch (cause) {
      setError(String(cause));
    } finally {
      setBusy(false);
    }
  }

  async function createChat() {
    if (!accountId) return;
    // Accept the space and initial key handoff before enabling message writes.
    const created = await db.insert(app.chats, { ownerId: accountId }).wait({ tier: "global" });
    const url = new URL(location.href);
    url.searchParams.set("chat", created.id);
    history.replaceState(null, "", url);
    setChatId(created.id);
  }

  async function sendMessage() {
    if (!chatId || !text.trim()) return;
    await db.insert(app.messages, { chatId, text: text.trim() }).wait({ tier: "global" });
    setText("");
  }

  async function shareChat() {
    if (!chatId || !recipient.trim()) return;
    await db.e2ee.spaces.grant(app.chats, chatId, recipient.trim()).wait();
    setStatus("Shared. Send the recipient the chat link.");
  }

  return (
    <main>
      <h1>Encrypted chat</h1>
      <p>
        Your account ID: <code>{accountId}</code>
      </p>
      <button disabled={busy || !accountId} onClick={() => void run(createChat)}>
        New chat
      </button>
      {error && <p role="alert">{error}</p>}
      {status && <p role="status">{status}</p>}
      {chat.error && <p role="alert">{chat.error.message}</p>}
      {chatId && (
        <>
          <p>
            Chat link: <a href={location.href}>{location.href}</a>
          </p>
          {messages.isLoading && <p>Loading messages…</p>}
          {messages.error && <p role="alert">{messages.error.message}</p>}
          <ul>
            {messages.data?.map((message) => (
              <li key={message.id}>{message.text}</li>
            ))}
          </ul>
          <form
            onSubmit={(event) => {
              event.preventDefault();
              void run(sendMessage);
            }}
          >
            <label>
              Message
              <input value={text} onChange={(event) => setText(event.target.value)} required />
            </label>
            <button disabled={busy || !accountId || !text.trim()}>Send</button>
          </form>
          {accountId && chat.data?.ownerId === accountId && (
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void run(shareChat);
              }}
            >
              <label>
                Recipient account ID
                <input
                  value={recipient}
                  onChange={(event) => setRecipient(event.target.value)}
                  required
                />
              </label>
              <button disabled={busy || !recipient.trim()}>Share chat</button>
            </form>
          )}
        </>
      )}
    </main>
  );
}
