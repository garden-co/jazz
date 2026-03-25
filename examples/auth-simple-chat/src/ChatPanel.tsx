import * as React from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/app";

export type ChatPanelProps = {
  chatId: string;
  title: string;
  canSend: boolean;
  authorName: string | null;
  placeholder?: string;
  readOnlyNotice?: string;
};

function formatTimestamp(date: Date): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function ChatPanel({
  chatId,
  title,
  canSend,
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
  const [messageError, setMessageError] = React.useState<string | null>(null);
  const [deletingMessageId, setDeletingMessageId] = React.useState<string | null>(null);
  const [deleteError, setDeleteError] = React.useState<string | null>(null);

  async function handleMessageSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSend || !sessionUserId || !authorName || !messageText.trim()) return;

    setMessagePending(true);
    setMessageError(null);
    setDeleteError(null);

    try {
      await db.insertDurable(
        app.messages,
        {
          author_id: sessionUserId,
          author_name: authorName,
          chat_id: chatId,
          text: messageText.trim(),
          sent_at: new Date(),
        },
        { tier: "edge" },
      );
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
      await db.deleteDurable(app.messages, messageId, { tier: "edge" });
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
