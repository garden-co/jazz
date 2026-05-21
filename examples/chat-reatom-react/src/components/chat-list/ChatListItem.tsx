import { reatomComponent } from "@reatom/react";
import { wrap } from "@reatom/core";
import { LockIcon, MessagesSquareIcon, TrashIcon } from "lucide-react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import { getChatDisplayName } from "@/model/chat-display-name";
import { chatRoute } from "@/routes";

interface ChatListItemProps {
  chatId: string;
  chat?: { id: string; isPublic: boolean; name?: string };
  onDelete: () => void;
}

export const ChatListItem = reatomComponent(({ chatId, chat, onDelete }: ChatListItemProps) => {
  const isPublic = chat?.isPublic ?? true;
  const displayName = getChatDisplayName(chatId, chat?.name)();

  return (
    <Item
      className="bg-background"
      variant="outline"
      size="sm"
      onClick={wrap(() => chatRoute.go({ chatId }))}
    >
      <ItemMedia>{isPublic ? <MessagesSquareIcon /> : <LockIcon />}</ItemMedia>
      <ItemContent>
        <ItemTitle>{displayName}</ItemTitle>
        <ItemDescription>
          {isPublic ? "Public " : "Private "}
          chat
        </ItemDescription>
      </ItemContent>
      <ItemActions>
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button variant="destructive" onClick={(evt) => evt.stopPropagation()}>
              <TrashIcon />
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
              <AlertDialogDescription>
                You will no longer see this chat in your list, but others will still be able to
                access it.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel onClick={(evt) => evt.stopPropagation()}>Cancel</AlertDialogCancel>
              <AlertDialogAction
                variant="destructive"
                onClick={(evt) => {
                  evt.stopPropagation();
                  evt.preventDefault();
                  onDelete();
                }}
              >
                Continue
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </ItemActions>
    </Item>
  );
}, "ChatListItem");
