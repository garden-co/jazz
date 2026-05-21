import { useCallback, useMemo, useRef, useState } from "react";
import { reatomComponent } from "@reatom/react";
import { SendIcon } from "lucide-react";
import type { DurabilityTier } from "jazz-tools";
import { ActionMenu } from "@/components/composer/ActionMenu";
import { Editor, type EditorHandle } from "@/components/editor/Editor";
import { Button } from "@/components/ui/button";
import { jazz } from "@/jazz";
import { myProfile } from "@/model/my-profile";
import { app } from "../../../schema.js";
import type { AttachmentData } from "./UploadModal";

interface MessageComposerProps {
  chatId: string;
  /** When true, the composer is locked regardless of internal readiness. */
  disabled?: boolean;
}

export const MessageComposer = reatomComponent(
  ({ chatId, disabled = false }: MessageComposerProps) => {
    const editorRef = useRef<EditorHandle>(null);
    const { db, session } = jazz();
    const userId = session?.user_id ?? null;
    const sharedWriteOptions: { tier: DurabilityTier } = useMemo(
      () => ({
        tier: db.getConfig().serverUrl ? "edge" : "local",
      }),
      [db],
    );

    const profile = myProfile();
    const [pendingSends, setPendingSends] = useState(0);
    const composerReady = !!userId && !!profile && !disabled && pendingSends === 0;

    const handleSend = useCallback(
      (html: string) => {
        if (!userId || !profile) return;
        if (!html.trim()) return;

        setPendingSends((count) => count + 1);
        void db
          .insert(app.messages, {
            chatId,
            text: html.trim(),
            senderId: profile.id,
            createdAt: new Date(),
          })
          .wait(sharedWriteOptions)
          .catch((error) => {
            console.error("failed to send message", error);
          })
          .finally(() => {
            setPendingSends((count) => Math.max(0, count - 1));
          });
      },
      [userId, chatId, db, profile, sharedWriteOptions],
    );

    const handleSendAttachment = useCallback(
      async (attachment: AttachmentData) => {
        if (!userId || !profile) {
          throw new Error("Profile is still loading. Please try again.");
        }

        const storedFile = await db.createFileFromBlob(app, attachment.file, sharedWriteOptions);

        const messageWriteResult = db.insert(app.messages, {
          chatId,
          text: "",
          senderId: profile.id,
          createdAt: new Date(),
        });
        const message = messageWriteResult.value;

        const attachmentWriteResult = db.insert(app.attachments, {
          messageId: message.id,
          type: attachment.type,
          name: attachment.file.name,
          fileId: storedFile.id,
          size: attachment.file.size,
        });

        if (sharedWriteOptions) {
          await Promise.all(
            [messageWriteResult, attachmentWriteResult].map((handle) =>
              handle.wait(sharedWriteOptions),
            ),
          );
        }
      },
      [userId, chatId, db, profile, sharedWriteOptions],
    );

    return (
      <div
        className="m-2 flex items-end gap-2"
        data-testid="message-composer"
        data-pending-sends={pendingSends}
      >
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
  },
  "MessageComposer",
);
