import * as React from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";
import { canInsertChatMessage, chatMessageInput } from "./chat-permissions";

export type ChatPanelProps = {
  chatId: string;
  title: string;
  authorName: string | null;
  placeholder?: string;
  readOnlyNotice?: string;
};

function formatTimestamp(date: Date | number): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function ChatPanel({
  chatId,
  title,
  authorName,
  placeholder,
  readOnlyNotice,
}: ChatPanelProps) {
  const db = useDb();
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;

  const rows =
    useAll(
      app.messages
        .where({ chat_id: chatId })
        .select("id", "author_name", "text", "sent_at", "$canDelete")
        .orderBy("sent_at", "asc"),
    ) ?? [];

  const [messageText, setMessageText] = React.useState("");
  const [messagePending, setMessagePending] = React.useState(false);
  const [canSend, setCanSend] = React.useState(false);
  const [messageError, setMessageError] = React.useState<string | null>(null);
  const [deletingMessageId, setDeletingMessageId] = React.useState<string | null>(null);
  const [deleteError, setDeleteError] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;

    if (!sessionUserId || !authorName) {
      setCanSend(false);
      return;
    }

    setCanSend(false);
    void canInsertChatMessage(db, app.messages, chatMessageInput(chatId, authorName, "")).then(
      (allowed) => {
        if (!cancelled) {
          setCanSend(allowed);
        }
      },
      () => {
        if (!cancelled) {
          setCanSend(false);
        }
      },
    );

    return () => {
      cancelled = true;
    };
  }, [authorName, chatId, db, session, sessionUserId]);

  async function handleMessageSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedText = messageText.trim();
    if (!sessionUserId || !authorName || !trimmedText) return;

    setMessagePending(true);
    setMessageError(null);
    setDeleteError(null);

    try {
      const message = chatMessageInput(chatId, authorName, trimmedText);
      if (!(await canInsertChatMessage(db, app.messages, message))) {
        setCanSend(false);
        setMessageError("You cannot send messages in this chat.");
        return;
      }

      await db.insert(app.messages, message).wait({ tier: "edge" });
      setMessageText("");
    } catch (error) {
      setMessageError(error instanceof Error ? error.message : String(error));
    } finally {
      setMessagePending(false);
    }
  }

  async function handleDeleteMessage(messageId: string) {
    setDeletingMessageId(messageId);
    setDeleteError(null);

    try {
      await db.delete(app.messages, messageId).wait({ tier: "edge" });
    } catch (error) {
      setDeleteError(error instanceof Error ? error.message : String(error));
    } finally {
      setDeletingMessageId(null);
    }
  }

  return (
    <section className="chat-card">
      <header className="chat-header">
        <h2>{title}</h2>
      </header>

      {!canSend && readOnlyNotice ? (
        <p className="chat-readonly-notice" data-testid={`chat-readonly-notice-${title}`}>
          {readOnlyNotice}
        </p>
      ) : null}

      {deleteError ? (
        <p className="error-text" data-testid={`delete-error-${title}`}>
          Delete failed: {deleteError}
        </p>
      ) : null}

      <ul className="message-list" data-testid={`message-list-${title}`}>
        {rows.length === 0 ? (
          <li className="empty-state">No messages yet.</li>
        ) : (
          rows.map((row) => (
            <li key={row.id} className="message-item" data-testid="message-item">
              <div className="message-meta">
                <div>
                  <strong data-testid="message-author">{row.author_name}</strong>
                  <time data-testid="message-date">{formatTimestamp(row.sent_at)}</time>
                </div>
                {row.$canDelete ? (
                  <button
                    type="button"
                    className="delete-message-button"
                    data-testid="delete-message-button"
                    disabled={deletingMessageId === row.id}
                    onClick={() => {
                      void handleDeleteMessage(row.id);
                    }}
                  >
                    {deletingMessageId === row.id ? "…" : "Delete"}
                  </button>
                ) : null}
              </div>
              <p data-testid="message-text">{row.text}</p>
            </li>
          ))
        )}
      </ul>

      {messageError ? (
        <p className="composer-error" data-testid={`message-error-${title}`}>
          {messageError}
        </p>
      ) : null}
      <form className="composer" onSubmit={handleMessageSubmit}>
        <input
          type="text"
          data-testid={`message-input-${title}`}
          value={messageText}
          onChange={(event) => setMessageText(event.target.value)}
          placeholder={placeholder ?? (canSend ? "Write a message..." : "You cannot send here")}
          disabled={!canSend || messagePending}
        />
        <button
          type="submit"
          className="composer-send"
          data-testid={`send-button-${title}`}
          disabled={!canSend || messagePending || !messageText.trim()}
        >
          {messagePending ? "..." : "Send"}
        </button>
      </form>
    </section>
  );
}
