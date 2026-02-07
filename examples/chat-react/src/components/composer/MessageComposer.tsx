import type { co } from "jazz-tools";
import { useCallback, useEffect, useRef } from "react";
import { SendIcon } from "lucide-react";
import { toast } from "sonner";
import { ActionMenu } from "@/components/composer/ActionMenu";
import { Editor } from "@/components/editor/Editor";
import { Button } from "@/components/ui/button";
import { type Attachment, type Chat, Message } from "@/schema";

interface MessageComposerProps {
  chat: co.loaded<typeof Chat>;
}

export function MessageComposer({ chat }: MessageComposerProps) {
  const messageRef = useRef(
    Message.create({ text: "", reactions: [] }, { owner: chat.$jazz.owner }),
  );
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Create a new message when the account or chat changes.
  useEffect(() => {
    messageRef.current = Message.create(
      { text: "", reactions: [] },
      { owner: chat.$jazz.owner },
    );
  }, [chat.$jazz.owner]);

  const handleSend = useCallback(() => {
    if (!messageRef.current.text.trim() && !messageRef.current.attachment) {
      console.log("No message or attachment");
      return;
    }
    chat.$jazz.push(messageRef.current);
    if (textareaRef.current) textareaRef.current.value = "";
    messageRef.current = Message.create(
      { text: "", reactions: [] },
      { owner: chat.$jazz.owner },
    );
  }, [chat]);

  const handleAttachmentUpload = useCallback(
    async (attachment: co.loaded<typeof Attachment>) => {
      try {
        const newMessage = Message.create(
          {
            text: "",
            reactions: [],
            attachment: attachment,
          },
          { owner: chat.$jazz.owner },
        );
        chat.$jazz.push(newMessage);
      } catch (err) {
        console.error(err);
        toast.error("Couldn't upload the file");
      }
    },
    [chat],
  );

  return (
    <div className="m-2 flex items-end gap-2">
      <ActionMenu
        onAddAttachment={handleAttachmentUpload}
        chatId={chat.$jazz.id}
      />

      <Editor message={messageRef} onEnter={handleSend} />

      <Button variant="outline" size="icon-lg" onClick={handleSend}>
        <SendIcon />
      </Button>
    </div>
  );
}
