import { useState } from "react";
import { SmilePlus } from "lucide-react";
import { useDb, useSession } from "jazz-tools/react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { app } from "../../../schema/app.js";

interface ReactionPickerProps {
  onPick: (emoji: string) => void;
  messageId: string;
}

export const ReactionPicker = ({ onPick, messageId }: ReactionPickerProps) => {
  const [customEmoji, setCustomEmoji] = useState("🍉");
  const db = useDb();
  const session = useSession();
  const presets = ["❤️", "👍", "🔥", "😂", "😮", "😢"];
  const emojiRegex =
    /^(\p{Extended_Pictographic}|\p{Emoji_Component}|\p{Emoji_Presentation}|\s)+$/u;

  const addReaction = (emoji: string) => {
    if (!session?.user_id) return;
    db.insert(app.reactions, {
      messageId,
      userId: session.user_id,
      emoji,
    });
    onPick(emoji);
  };

  return (
    <DropdownMenuSub>
      <DropdownMenuSubTrigger>
        <SmilePlus />
        React
      </DropdownMenuSubTrigger>

      <DropdownMenuSubContent className="w-36">
        <DropdownMenuLabel>Quick Reactions</DropdownMenuLabel>
        <div className="flex flex-wrap gap-2 p-2">
          {presets.map((emoji) => (
            <Button variant="outline" key={emoji} onClick={() => addReaction(emoji)}>
              {emoji}
            </Button>
          ))}
        </div>
        <DropdownMenuSeparator />
        <div className="p-2">
          <label className="text-xs text-muted-foreground mb-1 block" htmlFor="customEmoji">
            Custom
          </label>
          <div className="flex gap-2">
            <Input
              id="customEmoji"
              value={customEmoji}
              onChange={(e) => setCustomEmoji(e.target.value)}
              className="h-8"
              maxLength={2}
            />
            <Button
              size="sm"
              variant="outline"
              className="h-8 px-2"
              disabled={!customEmoji || !emojiRegex.test(customEmoji)}
              onClick={() => {
                addReaction(customEmoji);
                setCustomEmoji("");
              }}
            >
              Add
            </Button>
          </div>
        </div>
      </DropdownMenuSubContent>
    </DropdownMenuSub>
  );
};
