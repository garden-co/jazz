import { useRef, useState } from "react";
import { SendIcon } from "lucide-react";
import { useDb } from "jazz-tools/react";
import { Button } from "../ui/button.js";
import { IMAGE_TYPES, MAX_IMAGE_BYTES, sendMessage } from "../../chat.js";

// Reuses chat-react's bottom composer layout and outline/icon Send affordance.
// Plain text keeps this example independent of rich-text sanitisation/editor plugins.
export function MessageComposer({
  chatId,
  accountId,
  onSaved,
}: {
  chatId: string;
  accountId: string;
  onSaved: (id: string, accepted: Promise<unknown>) => void;
}) {
  const db = useDb();
  const [text, setText] = useState("");
  const [image, setImage] = useState<File | null>(null);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [sending, setSending] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const submission = useRef(0);
  return (
    <form
      className="m-2 flex flex-col gap-2"
      data-testid="message-composer"
      onSubmit={async (event) => {
        event.preventDefault();
        if (sending) return;
        const current = ++submission.current;
        setSending(true);
        setError("");
        setStatus("");
        try {
          const write = await sendMessage(db, chatId, accountId, text, image, setStatus);
          setStatus("Saved on this device");
          const accepted = write.wait({ tier: "global" });
          onSaved(write.value.id, accepted);
          void accepted.then(
            () => {
              if (submission.current === current) setStatus("Accepted by server");
            },
            () => {
              if (submission.current === current)
                setStatus("Saved on this device · acceptance unconfirmed");
            },
          );
          setText("");
          setImage(null);
          if (input.current) input.current.value = "";
        } catch (cause) {
          setStatus("Failed");
          setError(
            `${cause instanceof Error ? cause.message : String(cause)}. Your draft is retained. Check encryption and the room history before explicitly resending: a failed handoff is not proof of rollback.`,
          );
        } finally {
          setSending(false);
        }
      }}
    >
      <div className="flex items-end gap-2">
        <textarea
          aria-label="Message"
          placeholder="Write an encrypted message…"
          className="flex-1 min-w-0 rounded-md border bg-background p-3 text-foreground resize-none"
          rows={2}
          value={text}
          onChange={(event) => setText(event.target.value)}
          disabled={sending}
        />
        <Button
          variant="outline"
          size="icon-lg"
          type="submit"
          aria-label="Send"
          disabled={sending || (!text.trim() && !image)}
        >
          <SendIcon />
        </Button>
      </div>
      <div className="flex flex-wrap gap-2 items-center text-sm">
        <label>
          Image attachment{" "}
          <input
            ref={input}
            aria-label="Image attachment"
            type="file"
            accept={IMAGE_TYPES.join(",")}
            disabled={sending}
            onChange={(event) => {
              const selected = event.target.files?.[0] ?? null;
              setError("");
              setStatus("");
              if (
                selected &&
                (!IMAGE_TYPES.some((type) => type === selected.type) ||
                  selected.size > MAX_IMAGE_BYTES ||
                  selected.size === 0)
              ) {
                setImage(null);
                event.target.value = "";
                setError("Choose a nonempty PNG, JPEG, WebP or GIF image up to 10 MiB");
              } else setImage(selected);
            }}
          />
        </label>
        {image && (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            disabled={sending}
            onClick={() => {
              setImage(null);
              if (input.current) input.current.value = "";
            }}
          >
            Remove image
          </Button>
        )}
        <span data-testid="send-status" aria-live="polite">
          {status}
        </span>
      </div>
      {error && (
        <p role="alert" className="text-destructive text-sm">
          {error}
        </p>
      )}
    </form>
  );
}
