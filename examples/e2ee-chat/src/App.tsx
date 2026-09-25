import { useEffect, useState } from "react";
import { ReadTier, type DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import { LockKeyholeIcon, PlusIcon } from "lucide-react";
import { app } from "../schema.js";
import { prepareAccountConfig } from "./account.js";
import { createChat, shareChat } from "./chat.js";
import { Button } from "./components/ui/button.js";
import { ChatMessage } from "./components/chat/ChatMessage.js";
import { MessageComposer } from "./components/composer/MessageComposer.js";

export function App() {
  const [config, setConfig] = useState<DbConfig>();
  const [error, setError] = useState("");
  useEffect(() => {
    let cancelled = false;
    prepareAccountConfig().then(
      (value) => {
        if (!cancelled) setConfig(value);
      },
      (cause) => {
        if (!cancelled) setError(String(cause));
      },
    );
    return () => {
      cancelled = true;
    };
  }, []);
  if (error)
    return (
      <p role="alert" className="p-8">
        {error}
      </p>
    );
  if (!config) return <p className="p-8">Preparing your account…</p>;
  return (
    <JazzProvider config={config} fallback={<p className="p-8">Opening encrypted chat…</p>}>
      <ChatApp />
    </JazzProvider>
  );
}

function ChatApp() {
  const db = useDb();
  const session = useSession();
  const accountId = session?.user.account;
  const [error, setError] = useState("");
  const [creating, setCreating] = useState(false);
  const [chatId, setChatId] = useState(() => new URLSearchParams(location.search).get("chat"));
  const rooms = useAll(app.chats, { tier: ReadTier.LocalFirst });
  useEffect(() => {
    const onNavigation = () => setChatId(new URLSearchParams(location.search).get("chat"));
    window.addEventListener("popstate", onNavigation);
    return () => window.removeEventListener("popstate", onNavigation);
  }, []);
  const selectChat = (id: string | null) => {
    const url = new URL(location.href);
    if (id) url.searchParams.set("chat", id);
    else url.searchParams.delete("chat");
    history.pushState(null, "", url);
    setChatId(id);
    setError("");
  };
  return (
    <main className="flex flex-col h-dvh bg-muted text-muted-foreground">
      <nav className="flex flex-wrap items-center justify-between gap-3 border-b bg-background px-4 py-3">
        <Button variant="ghost" onClick={() => selectChat(null)}>
          <LockKeyholeIcon />
          <span className="font-manrope font-bold">Encrypted Jazz chat</span>
        </Button>
        <div className="text-xs break-all">
          Your account:{" "}
          {accountId ? <code data-testid="account-id">{accountId}</code> : "Preparing device…"}
        </div>
        <Button
          disabled={!accountId || creating}
          onClick={async () => {
            if (!accountId) return;
            setCreating(true);
            setError("");
            try {
              const { chat, completion } = await createChat(db, accountId);
              selectChat(chat.id);
              void completion.wait({ tier: "global" }).catch((cause) => {
                setError(
                  `Server confirmation is unavailable: ${String(cause)}. Your local chat is retained; inspect it before creating another.`,
                );
              });
            } catch (cause) {
              setError(
                `${String(cause)}. A failed handoff does not imply rollback; inspect your chat list before creating another room.`,
              );
            } finally {
              setCreating(false);
            }
          }}
        >
          <PlusIcon />
          New encrypted chat
        </Button>
      </nav>
      {error && (
        <p role="alert" className="p-3 text-destructive">
          {error}
        </p>
      )}
      {accountId &&
        (chatId ? (
          /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(chatId) ? (
            <ChatView key={chatId} chatId={chatId} accountId={accountId} />
          ) : (
            <p role="alert" className="p-8">
              Invalid chat ID
            </p>
          )
        ) : (
          <section className="mx-auto w-full max-w-2xl p-6">
            <h1 className="text-xl text-foreground mb-2">Your encrypted chats</h1>
            <p className="mb-4">
              Create a chat, then share it with another account ID. Opening a room link never grants
              access.
            </p>
            {rooms.error && <p role="alert">{rooms.error.message}</p>}
            {rooms.isLoading && <p>Loading chats…</p>}
            <ul className="flex flex-col gap-2">
              {rooms.data?.map((room) => (
                <li key={room.id}>
                  <Button
                    className="w-full justify-start"
                    variant="outline"
                    onClick={() => selectChat(room.id)}
                  >
                    Encrypted chat · {room.id.slice(0, 8)}
                  </Button>
                </li>
              ))}
            </ul>
            <p className="text-sm mt-6">
              Text and image content, filenames and MIME types are encrypted. Account/room IDs,
              membership, timestamps, sizes and access patterns are not private.
            </p>
          </section>
        ))}
    </main>
  );
}

function ChatView({ chatId, accountId }: { chatId: string; accountId: string }) {
  const db = useDb();
  const rooms = useAll(app.chats.where({ id: chatId }), { tier: ReadTier.LocalFirst });
  const remoteRooms = useAll(app.chats.select("id").where({ id: chatId }), {
    tier: ReadTier.Remote,
  });
  const room = rooms.data?.[0];
  const messages = useAll(
    room
      ? app.messages.select("*", "$createdAt").where({ chatId }).orderBy("$createdAt", "desc")
      : undefined,
    { tier: ReadTier.LocalFirst },
  );
  const remoteMessages = useAll(room ? app.messages.select("id").where({ chatId }) : undefined, {
    tier: ReadTier.Remote,
  });
  const remoteIds = new Set(remoteMessages.data?.map((message) => message.id));
  const [receipts, setReceipts] = useState<Record<string, string>>({});
  const onSaved = (id: string, accepted: Promise<unknown>) => {
    setReceipts((current) => ({ ...current, [id]: "Saved on this device · pending acceptance" }));
    void accepted.then(
      () => setReceipts((current) => ({ ...current, [id]: "Accepted by server" })),
      (cause) => {
        setReceipts((current) => ({
          ...current,
          [id]: "Saved on this device · acceptance unconfirmed",
        }));
        setError(`${String(cause)}. Inspect the existing message before resending.`);
      },
    );
  };
  const [recipient, setRecipient] = useState("");
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  if (rooms.error)
    return (
      <p role="alert" className="p-8">
        {rooms.error.message}
      </p>
    );
  if (rooms.isLoading) return <p className="p-8">Loading chat…</p>;
  if (!room) {
    if (remoteRooms.error)
      return (
        <p role="alert" className="p-8">
          Chat is not available locally. Server lookup failed: {remoteRooms.error.message}
        </p>
      );
    if (remoteRooms.isLoading)
      return <p className="p-8">Chat is not available locally. Waiting for the server…</p>;
    if (remoteRooms.data?.some((remoteRoom) => remoteRoom.id === chatId))
      return <p className="p-8">Chat is available from the server. Waiting for local data…</p>;
    return <p className="p-8">You don't have permission to access this chat.</p>;
  }
  return (
    <>
      <header className="border-b px-4 py-3 bg-background flex flex-col gap-2">
        <h1 className="text-foreground">Encrypted chat · {chatId.slice(0, 8)}</h1>
        <div className="text-xs break-all">
          Room ID: <code data-testid="chat-id">{chatId}</code>
        </div>
        <p className="text-xs">
          Local visibility is not acceptance. “Available from server” does not confirm Global
          settlement or that a recipient has read the message.
        </p>
        {remoteMessages.error && (
          <p className="text-xs">Server confirmation unavailable; local messages remain visible.</p>
        )}
        {room.ownerId === accountId && (
          <form
            className="flex flex-wrap items-center gap-2"
            onSubmit={async (event) => {
              event.preventDefault();
              if (busy) return;
              setBusy(true);
              setError("");
              setStatus("");
              try {
                await shareChat(db, chatId, recipient.trim());
                setStatus("Chat shared. Send the recipient this page's URL.");
              } catch (cause) {
                setError(
                  `${String(cause)}. Membership may already be accepted. Ask the recipient to open the app, then explicitly retry Share chat. This does not roll back accepted changes.`,
                );
              } finally {
                setBusy(false);
              }
            }}
          >
            <input
              className="min-w-0 flex-1 max-w-md border rounded-sm p-2 bg-background text-sm"
              aria-label="Recipient account ID"
              placeholder="Recipient account ID"
              value={recipient}
              onChange={(event) => setRecipient(event.target.value)}
              disabled={busy}
            />
            <Button variant="outline" type="submit" disabled={busy || !recipient.trim()}>
              Share chat
            </Button>
          </form>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            disabled={busy}
            onClick={async () => {
              setBusy(true);
              setError("");
              setStatus("");
              try {
                const state = await db.e2ee.explain({ scope: app.chats, identifier: chatId });
                setStatus(
                  `Encryption: ${state.state}${state.reason ? ` (${state.reason})` : ""}. If a previous read failed, reload this chat after maintenance.`,
                );
              } catch (cause) {
                setError(String(cause));
              } finally {
                setBusy(false);
              }
            }}
          >
            Check encryption
          </Button>
          <Button variant="ghost" size="sm" onClick={() => location.reload()}>
            Reload chat
          </Button>
          <p role="status" className="text-sm">
            {status}
          </p>
        </div>
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto flex flex-col-reverse p-2 gap-8 pb-6">
        {messages.error ? (
          <p role="alert">
            Unable to authenticate or read messages: {messages.error.message}. Use Check encryption;
            no unauthenticated content is displayed.
          </p>
        ) : (
          messages.data?.map((message) => (
            <ChatMessage
              key={message.id}
              message={message}
              isMe={message.senderId === accountId}
              status={
                receipts[message.id] === "Accepted by server"
                  ? "Accepted by server"
                  : remoteIds.has(message.id)
                    ? "Available from server"
                    : (receipts[message.id] ?? "Local · acceptance unconfirmed")
              }
            />
          ))
        )}
        {messages.isLoading && <p>Loading encrypted messages…</p>}
      </div>
      <MessageComposer chatId={chatId} accountId={accountId} onSaved={onSaved} />
    </>
  );
}
