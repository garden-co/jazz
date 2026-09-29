"use client";

import { useRef, useState } from "react";
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

  async function send(value: string) {
    const body = value.trim();
    if (!body && files.length === 0) return;
    const outgoing = files;
    setSending(true);
    setText("");
    setFiles([]);
    setError(null);
    try {
      const base = { roomId, senderId: profileId };
      if (outgoing.length === 0) {
        db.insert(app.messages, { ...base, text: body });
      } else {
        // Each file becomes its own message; the text rides on the first one.
        // Bytes stream into the row instead of being copied through memory.
        for (const [index, { file }] of outgoing.entries()) {
          await db.insertStreaming(app.messages, {
            ...base,
            text: index === 0 ? body : "",
            attachmentName: file.name,
            attachmentType: file.type,
            attachmentSize: file.size,
            attachment: file.stream(),
          });
        }
      }
      // Members may record activity on the room; the policy keeps its name fixed.
      db.update(app.rooms, roomId, { lastActivityAt: new Date() });
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
