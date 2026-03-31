import { Suspense, useState } from "react";
import { useAll } from "jazz-tools/react";
import { SettingsIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useChatDisplayName } from "@/hooks/useChatDisplayName";
import { inIframe } from "@/lib/utils";
import { ChatSettings } from "./ChatSettings";
import { app } from "../../../schema.js";

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

function ChatHeaderContent({ chatId }: ChatHeaderProps) {
  const [settingsOpen, setSettingsOpen] = useState(false);

  const chatRows = useAll(app.chats.where({ id: chatId })) ?? [];
  const chat = chatRows[0];

  const displayName = useChatDisplayName(chatId, chat?.name);

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
}
