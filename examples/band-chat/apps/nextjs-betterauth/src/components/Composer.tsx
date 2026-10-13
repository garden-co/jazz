"use client";

import { useEffect, useRef, useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Button,
  ChatComposer,
  ChatComposerDrawer,
  ChatSendButton,
  HStack,
  Thumbnail,
  Token,
} from "@astryxdesign/core";
import { app } from "../../schema";
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
  // A send commits locally first, so it works offline and clears the draft
  // at once. The server can still reject it later, for example if a policy
  // denies it once the sender was removed from the room, so each message is
  // tracked until the server accepts or rejects it.
  async function send(value: string) {
    const body = value.trim();
    if ((!body && files.length === 0) || isSending) return;
    const outgoing = files;
    setSending(true);
    setError(null);
    try {
      const base = { roomId, senderId: profileId };
      if (outgoing.length === 0) {
        // The room list picks the message up as the room's newest; nothing
        // else is written.
        const committed = db.insert(app.messages, { ...base, text: body });
        unsent.track(committed, { roomId, roomName, text: body });
        setText("");
        return;
      }
      // Each file becomes its own message; the text rides on the first one.
      // Bytes stream into the row instead of being copied through memory.
      for (const [index, { key, file }] of outgoing.entries()) {
        const inserted = await db.insertStreaming(app.messages, {
          ...base,
          text: index === 0 ? body : "",
          attachmentName: file.name,
          attachmentType: file.type,
          attachmentSize: file.size,
          attachment: file.stream(),
        });
        unsent.track(inserted, {
          roomId,
          roomName,
          text: index === 0 ? body : "",
          attachmentName: file.name,
        });
        if (index === 0) setText("");
        setFiles((current) => current.filter((item) => item.key !== key));
      }
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
