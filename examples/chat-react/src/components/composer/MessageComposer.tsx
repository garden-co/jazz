import { useCallback, useRef } from "react";
import { SendIcon } from "lucide-react";
import { useDb, useSession } from "jazz-tools/react";
import { ActionMenu } from "@/components/composer/ActionMenu";
import { Editor, type EditorHandle } from "@/components/editor/Editor";
import { Button } from "@/components/ui/button";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../../schema/app.js";
import type { AttachmentData } from "./UploadModal";

interface MessageComposerProps {
  chatId: string;
}

export function MessageComposer({ chatId }: MessageComposerProps) {
  const editorRef = useRef<EditorHandle>(null);
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;

  const myProfile = useMyProfile();

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
      if (!userId || !myProfile) return;

      const storedFile = await db.createFileFromBlob(app, attachment.file);

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
    },
    [userId, chatId, db, myProfile],
  );

  return (
    <div className="m-2 flex items-end gap-2">
      <ActionMenu chatId={chatId} onAttachment={handleSendAttachment} />

      <Editor ref={editorRef} onSend={handleSend} disabled={!userId} />

      <Button
        variant="outline"
        size="icon-lg"
        onClick={() => editorRef.current?.send()}
        disabled={!userId}
      >
        <SendIcon />
      </Button>
    </div>
  );
}
