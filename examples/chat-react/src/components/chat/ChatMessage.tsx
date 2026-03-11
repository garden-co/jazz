import { useState } from "react";
import DOMPurify from "dompurify";
import { TrashIcon } from "lucide-react";
import { useAll } from "jazz-tools/react";
import { ChatMetadata } from "@/components/chat/ChatMetadata";
import { ChatImage } from "@/components/chat/ChatImage";
import { ChatFile } from "@/components/chat/ChatFile";
import { CollaborativeCanvas } from "@/components/canvas/Canvas";
import { ReactionPicker } from "@/components/chat/ChatReactionPicker";
import { MessageReactions } from "@/components/chat/ChatReactions";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Item, ItemContent } from "@/components/ui/item";
import { app, type Message, type Profile } from "../../../schema/app.js";

interface ChatMessageProps {
  message: Message;
  sender?: Profile;
  isMe: boolean;
  onDelete: () => void;
}

export const ChatMessage = ({ message, sender, isMe, onDelete }: ChatMessageProps) => {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);

  // Subscribe directly to the sender's profile so the name/avatar appears as
  // soon as the profile row syncs, even if the include in the parent query
  // fired before the profile was in the local store.
  const senderProfiles = useAll(app.profiles.where({ userId: message.senderId })) ?? [];
  const resolvedSender = (senderProfiles[0] as unknown as Profile) ?? sender;

  const attachments = useAll(app.attachments.where({ message: message.id })) ?? [];

  const canvasMatch = message.text?.match(/^\[Canvas: ([^\]]+)\]$/);
  const canvasId = canvasMatch?.[1] ?? null;

  const sanitisedHtml = message.text && !canvasId ? DOMPurify.sanitize(message.text) : null;

  return (
    <>
      <article
        className={`max-w-7/8 flex flex-col ${
          isMe ? "self-end items-end" : "self-start items-start"
        }`}
      >
        <ChatMetadata date={message.createdAt} senderName={resolvedSender?.name} />

        <DropdownMenu open={isMenuOpen} onOpenChange={setIsMenuOpen}>
          <DropdownMenuTrigger asChild>
            <Item
              variant="outline"
              className={`
                max-w-full inline-flex px-2 pt-0 py-1 shadow-xs cursor-pointer select-none
                ${isMe ? "border-0 bg-primary-500 text-white" : "bg-background"}
              `}
            >
              <ItemContent className="mt-0 text-base relative">
                {canvasId ? (
                  <CollaborativeCanvas canvasId={canvasId} />
                ) : sanitisedHtml ? (
                  <div
                    className="prose prose-sm max-w-full wrap-anywhere [&>p]:my-0"
                    dangerouslySetInnerHTML={{ __html: sanitisedHtml }}
                  />
                ) : null}

                {attachments.map((att) =>
                  att.type === "image" ? (
                    <ChatImage key={att.id} attachment={att} />
                  ) : (
                    <ChatFile key={att.id} attachment={att} />
                  ),
                )}

                <MessageReactions messageId={message.id} isMe={isMe} />
              </ItemContent>
            </Item>
          </DropdownMenuTrigger>
          <DropdownMenuContent align={isMe ? "end" : "start"}>
            <ReactionPicker
              onPick={(_emoji) => {
                setIsMenuOpen(false);
              }}
              messageId={message.id}
            />

            {isMe && (
              <DropdownMenuItem
                variant="destructive"
                onSelect={(evt) => {
                  evt.preventDefault();
                  setIsDeleteDialogOpen(true);
                  setIsMenuOpen(false);
                }}
              >
                <TrashIcon /> Delete
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </article>

      <AlertDialog open={isDeleteDialogOpen} onOpenChange={setIsDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. This will permanently delete your message from our
              servers.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                onDelete();
                setIsDeleteDialogOpen(false);
              }}
            >
              Yes, delete it
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
};
