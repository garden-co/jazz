import { agentLabel } from "@/src/agent/config";
import { ChatPage } from "@/src/ui/chat-page";

export const dynamic = "force-dynamic";

export default function Chat() {
  return <ChatPage agentLabel={agentLabel()} />;
}
