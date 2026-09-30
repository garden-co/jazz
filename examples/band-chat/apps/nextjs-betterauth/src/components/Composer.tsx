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
import { writeRejectionReason } from "../lib/write-rejection";

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

  // A send commits locally first, so it works offline and clears the draft
  // at once. The server can still reject it later, for example if a policy
  // denies it, so each write also waits for the global tier. Only a rejection
  // means the write won't sync: any other failure, such as the database
  // shutting down on a room switch or tab close, leaves it committed locally.
  function onRejected(
    handle: { wait(options: { tier: "global" }): Promise<unknown> },
    report: (reason: string) => void,
  ) {
    handle.wait({ tier: "global" }).catch((cause: unknown) => {
      const reason = writeRejectionReason(cause);
      if (reason === undefined) console.warn("Could not confirm a write with the server", cause);
      else report(reason);
    });
  }

  // A rejected message says so, and its text comes back if the draft is empty.
  function reportUnsent(body: string) {
    return (reason: string) => {
      setError(`Message not sent: ${reason}`);
      if (body) setText((current) => (current ? current : body));
    };
  }

  // The draft and pending files are cleared only once they are written
  // locally, so a send that fails here keeps what the person typed and any
  // files not yet sent.
  async function send(value: string) {
    const body = value.trim();
    if ((!body && files.length === 0) || isSending) return;
    const outgoing = files;
    setSending(true);
    setError(null);
    try {
      const base = { roomId, senderId: profileId };
      if (outgoing.length === 0) {
        // The message and the room's new activity commit together. Members
        // may record activity on the room; the policy keeps its name fixed.
        const committed = await db.transaction((tx) => {
          tx.insert(app.messages, { ...base, text: body });
          tx.update(app.rooms, roomId, { lastActivityAt: new Date() });
        });
        onRejected(committed, reportUnsent(body));
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
        onRejected(inserted, reportUnsent(index === 0 ? body : ""));
        if (index === 0) setText("");
        setFiles((current) => current.filter((item) => item.key !== key));
      }
      // The messages are already sent. A rejected activity update only leaves
      // the room lower in the list, so it is logged, not shown.
      onRejected(db.update(app.rooms, roomId, { lastActivityAt: new Date() }), (reason) =>
        console.warn(`Room activity not updated: ${reason}`),
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
