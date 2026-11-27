import { useEffect, useRef, useState } from "react";
import { id } from "@instantdb/react";
import {
  Button,
  ChatComposer,
  ChatComposerDrawer,
  ChatSendButton,
  HStack,
  Thumbnail,
  Token,
} from "@astryxdesign/core";
import { db, transact } from "../lib/db";
import { attachmentAccept, attachmentProblem, formatBytes, isImageType } from "../lib/attachments";
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
  profileAuthor,
  onStartSketch,
}: {
  roomId: string;
  roomName: string;
  profileId: string;
  /** The sender's account: attachments upload into its folder. */
  profileAuthor: string;
  onStartSketch: () => void;
}) {
  const unsent = useUnsentMessages();
  const fileInput = useRef<HTMLInputElement>(null);
  const [text, setText] = useState("");
  const [files, setFiles] = useState<PendingFile[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isSending, setSending] = useState(false);
  const canSend = (text.trim().length > 0 || files.length > 0) && !isSending;

  function choose(picked: FileList | null) {
    const accepted: PendingFile[] = [];
    const problems: string[] = [];
    for (const file of picked ?? []) {
      const problem = attachmentProblem(file);
      if (problem) problems.push(problem);
      else accepted.push({ key: crypto.randomUUID(), file });
    }
    setError(problems.length ? problems.join(" ") : null);
    setFiles((current) => [...current, ...accepted]);
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

  // The draft and pending files are cleared only once they are written
  // locally, so a send that fails here keeps what the person typed and any
  // files not yet sent.
  //
  // A send applies locally first (Instant's optimistic update) and clears the
  // draft at once. The server can still reject it, for example if a rule
  // denies it once the sender was removed from the room, so each message is
  // tracked until the server accepts or rejects it.
  async function send(value: string) {
    const body = value.trim();
    if ((!body && files.length === 0) || isSending) return;
    const outgoing = files;
    setSending(true);
    setError(null);
    try {
      const message = (fields: {
        text: string;
        attachmentName?: string;
        attachmentType?: string;
        attachmentSize?: number;
      }) =>
        db.tx.messages[id()]
          .create({ ...fields, createdAt: Date.now() })
          .link({ room: roomId, sender: profileId });
      if (outgoing.length === 0) {
        // The message and the room's new activity commit together. Members
        // may record activity on the room; the rules keep its name fixed.
        const committed = transact([
          message({ text: body }),
          db.tx.rooms[roomId].update({ lastActivityAt: Date.now() }),
        ]);
        unsent.track(committed, { roomId, roomName, text: body });
        setText("");
        return;
      }
      // Each file becomes its own message; the text rides on the first one.
      // The file goes to Instant Storage, into the sender's own folder.
      for (const [index, { key, file }] of outgoing.entries()) {
        const { data: upload } = await db.storage.uploadFile(
          `${profileAuthor}/${crypto.randomUUID()}/${file.name}`,
          file,
          { contentType: file.type },
        );
        const inserted = transact(
          message({
            text: index === 0 ? body : "",
            attachmentName: file.name,
            attachmentType: file.type,
            attachmentSize: file.size,
          }).link({ attachment: upload.id }),
        );
        unsent.track(inserted, {
          roomId,
          roomName,
          text: index === 0 ? body : "",
          attachmentName: file.name,
        });
        if (index === 0) setText("");
        setFiles((current) => current.filter((item) => item.key !== key));
      }
      // The messages are already sent. A rejected activity update only leaves
      // the room lower in the list, so it is logged, not shown.
      transact(db.tx.rooms[roomId].update({ lastActivityAt: Date.now() })).catch((cause: unknown) =>
        console.warn(`Room activity not updated: ${cause}`),
      );
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
