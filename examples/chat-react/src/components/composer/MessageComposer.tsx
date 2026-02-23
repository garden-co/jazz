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
        chat: chatId,
        text: html.trim(),
        sender: myProfile.id,
        senderId: userId,
        createdAt: Math.floor(Date.now() / 1000),
      });
    },
    [userId, chatId, db, myProfile],
  );

  const handleSendAttachment = useCallback(
    (attachment: AttachmentData) => {
      if (!userId || !myProfile) return;

      const messageId = db.insert(app.messages, {
        chat: chatId,
        text: "",
        sender: myProfile.id,
        senderId: userId,
        createdAt: Math.floor(Date.now() / 1000),
      });

      db.insert(app.attachments, {
        message: messageId,
        type: attachment.type,
        name: attachment.name,
        data: attachment.data,
        mimeType: attachment.mimeType,
        size: attachment.size,
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
