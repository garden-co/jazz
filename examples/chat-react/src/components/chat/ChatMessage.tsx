import type { co, ID } from "jazz-tools";
import { Suspense, useState } from "react";
import DOMPurify from "dompurify";
import { useSuspenseAccount } from "jazz-tools/react";
import { TrashIcon } from "lucide-react";
import { CollaborativeCanvas } from "@/components/canvas/Canvas";
import { ChatFile } from "@/components/chat/ChatFile";
import { ChatImage } from "@/components/chat/ChatImage";
import { ChatMetadata } from "@/components/chat/ChatMetadata";
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
import { Item, ItemContent, ItemHeader } from "@/components/ui/item";
import { ChatAccount, type Message } from "@/schema";

interface ChatMessageProps {
  message: co.loaded<
    typeof Message,
    {
      text: true;
      attachment: true;
      reactions: true;
    }
  >;
  onDelete: (id: ID<typeof Message>) => Promise<void>;
}

export const ChatMessage = ({ message, onDelete }: ChatMessageProps) => {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);
  const me = useSuspenseAccount(ChatAccount);
  const isMe = message.$jazz.createdBy === me.$jazz.id;
  const sanitised = DOMPurify.sanitize(message.text.toString());

  const handleEmojiSelect = (emoji: string) => {
    // Toggle if my current reaction is being clicked on,
    // otherwise switch to the new reaction.
    const current = message.reactions.byMe?.value;
    message.reactions.$jazz.push(current === emoji ? "" : emoji);
  };

  const handleDeleteConfirm = async () => {
    await onDelete(message.$jazz.id);
    setIsDeleteDialogOpen(false);
  };

  return (
    <Suspense>
      <article
        className={`max-w-7/8 flex flex-col ${
          isMe ? "self-end items-end" : "self-start items-start"
        }`}
      >
        <ChatMetadata
          date={message.$jazz.createdAt}
          sender={message.$jazz.createdBy}
        />

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
                {message.attachment?.type === "image" && (
                  <ItemHeader>
                    <Suspense>
                      <ChatImage
                        imageId={message.attachment.attachment.$jazz.id}
                      />
                    </Suspense>
                  </ItemHeader>
                )}

                {message.attachment?.type === "file" && (
                  <Suspense>
                    <ChatFile fileId={message.attachment.attachment.$jazz.id} />
                  </Suspense>
                )}

                {message.attachment?.type === "canvas" && (
                  <Suspense>
                    <CollaborativeCanvas
                      canvasId={message.attachment.canvas.$jazz.id}
                      showControls={true}
                      className="w-full"
                    />
                  </Suspense>
                )}

                <div
                  className="whitespace-pre-line max-w-full wrap-anywhere"
                  // biome-ignore lint/security/noDangerouslySetInnerHtml: This string is sanitized
                  dangerouslySetInnerHTML={{ __html: sanitised }}
                />

                <MessageReactions
                  messageId={message.$jazz.id}
                  currentUserId={me.$jazz.id}
                  isMe={isMe}
                />
              </ItemContent>
            </Item>
          </DropdownMenuTrigger>
          <DropdownMenuContent align={isMe ? "end" : "start"}>
            <ReactionPicker
              onPick={(evt) => {
                handleEmojiSelect(evt);
                setIsMenuOpen(false);
              }}
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

      <AlertDialog
        open={isDeleteDialogOpen}
        onOpenChange={setIsDeleteDialogOpen}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. This will permanently delete your
              message from our servers.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={handleDeleteConfirm}
            >
              Yes, delete it
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Suspense>
  );
};
