import { useEffect, useRef, useState } from "react";
import { useMutation } from "convex/react";
import {
  Button,
  ChatComposer,
  ChatComposerDrawer,
  ChatSendButton,
  HStack,
  Thumbnail,
  Token,
} from "@astryxdesign/core";
import { api } from "@/convex/_generated/api";
import type { Id } from "../lib/model";
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
  roomId: Id<"rooms">;
  roomName: string;
  profileId: Id<"profiles">;
  onStartSketch: () => void;
}) {
  // Sending a message shows it at once, as in Convex's optimistic-update
  // guide: the room's newest page gets the message until the server's
  // result replaces it.
  const sendMessage = useMutation(api.messages.send).withOptimisticUpdate((store, args) => {
    const page = store.getQuery(api.messages.page, { roomId: args.roomId });
    if (page === undefined) return;
    const optimistic = {
      _id: crypto.randomUUID() as Id<"messages">,
      _creationTime: Date.now(),
      senderId: profileId,
      ...args,
    };
    store.setQuery(api.messages.page, { roomId: args.roomId }, [optimistic, ...page]);
  });
  const generateUploadUrl = useMutation(api.messages.generateUploadUrl);
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

  // The draft and pending files are cleared once a send is under way, so a
  // send that fails here keeps what the person typed and any files not yet
  // sent. The server can still reject a message, for example once the sender
  // was removed from the room, so each send is tracked until it settles.
  async function send(value: string) {
    const body = value.trim();
    if ((!body && files.length === 0) || isSending) return;
    const outgoing = files;
    setSending(true);
    setError(null);
    try {
      if (outgoing.length === 0) {
        // The message and the room's new activity commit together.
        unsent.track(sendMessage({ roomId, text: body }), { roomId, roomName, text: body });
        setText("");
        return;
      }
      // Each file becomes its own message; the text rides on the first one.
      // The bytes go to Convex file storage, the message keeps their id.
      for (const [index, { key, file }] of outgoing.entries()) {
        const uploadUrl = await generateUploadUrl();
        const upload = await fetch(uploadUrl, {
          method: "POST",
          headers: { "Content-Type": file.type },
          body: file,
        });
        if (!upload.ok) throw new Error(`Could not upload ${file.name}`);
        const { storageId } = (await upload.json()) as { storageId: Id<"_storage"> };
        unsent.track(
          sendMessage({
            roomId,
            text: index === 0 ? body : "",
            attachment: storageId,
            attachmentName: file.name,
            attachmentType: file.type,
            attachmentSize: file.size,
          }),
          { roomId, roomName, text: index === 0 ? body : "", attachmentName: file.name },
        );
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
