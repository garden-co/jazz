import type { ID } from "jazz-tools";
import { useSuspenseCoState } from "jazz-tools/react";
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
import { navigate } from "@/hooks/useRouter";
import { Chat } from "@/schema";

interface ChatListItemProps {
  chatId: ID<typeof Chat>;
  onDelete: () => void;
}

export function ChatListItem({ chatId, onDelete }: ChatListItemProps) {
  const chat = useSuspenseCoState(Chat, chatId);
  const isPublic = chat.$jazz.owner.getRoleOf("everyone");

  return (
    <Item
      className="bg-background"
      variant="outline"
      size="sm"
      onClick={() => navigate(`/chat/${chat.$jazz.id}`)}
    >
      <ItemMedia>{isPublic ? <MessagesSquareIcon /> : <LockIcon />}</ItemMedia>
      <ItemContent>
        <ItemTitle>
          {new Date(chat.$jazz.createdAt).toLocaleString(undefined, {
            year: "numeric",
            month: "short",
            day: "numeric",
            hour: "numeric",
            minute: "numeric",
            timeZoneName: "short",
            hour12: true,
            timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
          })}
        </ItemTitle>

        <ItemDescription>
          {isPublic ? "Public " : "Private "}
          chat
        </ItemDescription>
      </ItemContent>
      <ItemActions>
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button
              variant="destructive"
              onClick={(evt) => {
                evt.stopPropagation();
              }}
            >
              <TrashIcon />
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Are you absolutely sure?</AlertDialogTitle>
              <AlertDialogDescription>
                You will no longer see this chat in your list, but others will
                still be able to access it.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel onClick={(evt) => evt.stopPropagation()}>
                Cancel
              </AlertDialogCancel>
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
}
