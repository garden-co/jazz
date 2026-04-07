import { useCallback, useRef } from "react";
import { SendIcon } from "lucide-react";
import { FileNotFoundError, IncompleteFileDataError } from "jazz-tools";
import { useDb, useSession } from "jazz-tools/react";
import { ActionMenu } from "@/components/composer/ActionMenu";
import { Editor, type EditorHandle } from "@/components/editor/Editor";
import { Button } from "@/components/ui/button";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../../schema.js";
import type { AttachmentData } from "./UploadModal";

const FILE_READINESS_TIMEOUT_MS = 10_000;
const FILE_READINESS_POLL_INTERVAL_MS = 100;

function isTransientFileReadError(error: unknown): boolean {
  return (
    error instanceof FileNotFoundError ||
    (error instanceof IncompleteFileDataError && error.reason === "missing-part")
  );
}

async function waitForFileReadability(readFile: () => Promise<Blob>): Promise<void> {
  const deadline = Date.now() + FILE_READINESS_TIMEOUT_MS;

  while (true) {
    try {
      await readFile();
      return;
    } catch (error) {
      if (!isTransientFileReadError(error) || Date.now() >= deadline) {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, FILE_READINESS_POLL_INTERVAL_MS));
    }
  }
}

interface MessageComposerProps {
  chatId: string;
}

export function MessageComposer({ chatId }: MessageComposerProps) {
  const editorRef = useRef<EditorHandle>(null);
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  const myProfile = useMyProfile();
  const composerReady = !!userId && !!myProfile;

  const handleSend = useCallback(
    (html: string) => {
      if (!userId || !myProfile) return;
      if (!html.trim()) return;

      db.insert(app.messages, {
        chatId,
        text: html.trim(),
        senderId: myProfile.id,
        createdAt: new Date(),
      });
    },
    [userId, chatId, db, myProfile],
  );

  const handleSendAttachment = useCallback(
    async (attachment: AttachmentData) => {
      if (!userId || !myProfile) {
        throw new Error("Profile is still loading. Please try again.");
      }

      const storedFile = await db.createFileFromBlob(app, attachment.file, { tier: "edge" });

      const message = db.insert(app.messages, {
        chatId,
        text: "",
        senderId: myProfile.id,
        createdAt: new Date(),
      });

      db.insert(app.attachments, {
        messageId: message.id,
        type: attachment.type,
        name: attachment.file.name,
        fileId: storedFile.id,
        size: attachment.file.size,
      });

      // Only resolve the upload once the newly linked file is readable through
      // the same path the chat uses to render/download it.
      await waitForFileReadability(() => db.loadFileAsBlob(app, storedFile.id, { tier: "edge" }));
    },
    [userId, chatId, db, myProfile],
  );

  return (
    <div className="m-2 flex items-end gap-2">
      <ActionMenu chatId={chatId} onAttachment={handleSendAttachment} disabled={!composerReady} />

      <Editor ref={editorRef} onSend={handleSend} disabled={!composerReady} />

      <Button
        variant="outline"
        size="icon-lg"
        onClick={() => editorRef.current?.send()}
        disabled={!composerReady}
      >
        <SendIcon />
      </Button>
    </div>
  );
}
