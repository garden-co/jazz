import { Suspense, useState } from "react";
import { reatomComponent } from "@reatom/react";
import { SettingsIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { getChatDisplayName } from "@/model/chat-display-name";
import { getChatRowsQuery } from "@/model/queries";
import { inIframe } from "@/lib/utils";
import { ChatSettings } from "./ChatSettings";

interface ChatHeaderProps {
  chatId: string;
}

export function ChatHeader({ chatId }: ChatHeaderProps) {
  if (inIframe) return null;

  return (
    <Suspense fallback={null}>
      <ChatHeaderContent chatId={chatId} />
    </Suspense>
  );
}

const ChatHeaderContent = reatomComponent(({ chatId }: ChatHeaderProps) => {
  const [settingsOpen, setSettingsOpen] = useState(false);

  const chatRows = getChatRowsQuery(chatId)();
  const chat = chatRows[0];
  const displayName = getChatDisplayName(chatId, chat?.name ?? undefined)();

  return (
    <>
      <div
        data-testid="chat-header"
        className="px-3 py-2 flex items-center justify-between border-b"
      >
        <h3 className="text-sm font-semibold truncate">{displayName}</h3>
        <Button variant="ghost" size="icon" onClick={() => setSettingsOpen(true)}>
          <SettingsIcon />
        </Button>
      </div>

      <ChatSettings chatId={chatId} open={settingsOpen} onOpenChange={setSettingsOpen} />
    </>
  );
}, "ChatHeaderContent");
