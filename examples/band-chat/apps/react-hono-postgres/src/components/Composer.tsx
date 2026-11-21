import { useEffect, useRef, useState } from "react";
import { useDb } from "../data/live";
import {
  Button,
  ChatComposer,
  ChatComposerDrawer,
  ChatSendButton,
  HStack,
  Thumbnail,
  Token,
} from "@astryxdesign/core";
import { mutators } from "../data/mutators";
import { attachmentAccept, formatBytes, isImageType } from "../lib/attachments";
import { useObjectUrl } from "../lib/use-object-url";
import { useUnsentMessages } from "./UnsentNotices";

interface PendingFile {
  key: string;
  file: File;
}

export function Composer({
  roomId,
  roomName,
  profileId,
  onStartSketch,
}: {
  roomId: string;
  roomName: string;
  profileId: string;
  onStartSketch: () => void;
}) {
  const db = useDb();
  const unsent = useUnsentMessages();
  const fileInput = useRef<HTMLInputElement>(null);
  const [text, setText] = useState("");
  const [files, setFiles] = useState<PendingFile[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isSending, setSending] = useState(false);
  const canSend = (text.trim().length > 0 || files.length > 0) && !isSending;

  // Attachments are left out of the Postgres port (see NOTES.md): every pick is refused.
  function choose(picked: FileList | null) {
    const problems = [...(picked ?? [])].map((file) => `${file.name}: attachments are not available.`);
    setError(problems.length ? problems.join(" ") : null);
  }

  // A rejected message comes back into an empty draft while this room is
  // open. Its notice is shown above the rooms, so it stays even when the
  // rejection means this room is gone.
  useEffect(
    () =>
      unsent.onUnsent((message) => {
        if (message.roomId === roomId && message.text)
          setText((current) => (current ? current : message.text));
      }),
    [unsent, roomId],
  );

  // A send applies locally first and clears the draft at once. The server
  // can still reject it later, for example once the sender was removed from
  // the room, so each message is tracked until the server accepts or rejects
  // it. Attachments are left out of the Postgres port, so files are never sent.
  function send(value: string) {
    const body = value.trim();
    if (!body || isSending) return;
    setSending(true);
    setError(null);
    try {
      // The message and the room's new activity are one mutation.
      const write = db.mutate(
        mutators.messages.send({ id: crypto.randomUUID(), roomId, senderId: profileId, text: body }),
      );
      unsent.track(write, { roomId, roomName, text: body });
      setText("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setSending(false);
    }
  }

  return (
    <>
      <ChatComposer
        value={text}
        onChange={setText}
        onSubmit={(value) => void send(value)}
        placeholder={`Message ${roomName}`}
        status={error ? { type: "error", message: error } : undefined}
        drawer={
          files.length > 0 ? (
            <ChatComposerDrawer>
              <HStack gap={2} wrap="wrap">
                {files.map((pending) => (
                  <PendingAttachment
                    key={pending.key}
                    file={pending.file}
                    onRemove={() =>
                      setFiles((current) => current.filter((item) => item.key !== pending.key))
                    }
                  />
                ))}
              </HStack>
            </ChatComposerDrawer>
          ) : undefined
        }
        footerActions={
          <HStack gap={1}>
            <Button
              label="Attach"
              size="md"
              variant="ghost"
              onClick={() => fileInput.current?.click()}
            />
            <Button label="Sketch" size="md" variant="ghost" onClick={onStartSketch} />
          </HStack>
        }
        sendButton={<ChatSendButton isDisabled={!canSend} onSend={() => void send(text)} />}
      />
      {/* A plain hidden input: the composer's Attach button opens it and the
          drawer above lists the chosen files, so a visible FileInput field
          would duplicate both. */}
      <input
        ref={fileInput}
        type="file"
        multiple
        hidden
        accept={attachmentAccept}
        aria-label="Attachment"
        onChange={(event) => {
          choose(event.target.files);
          event.target.value = "";
        }}
      />
    </>
  );
}

function PendingAttachment({ file, onRemove }: { file: File; onRemove: () => void }) {
  const src = useObjectUrl(isImageType(file.type) ? file : null);
  if (src) return <Thumbnail src={src} alt={file.name} label={file.name} onRemove={onRemove} />;
  return <Token label={file.name} description={formatBytes(file.size)} onRemove={onRemove} />;
}
