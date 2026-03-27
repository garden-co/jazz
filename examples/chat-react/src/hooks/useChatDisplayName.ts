import { useAll, useSession } from "jazz-tools/react";
import { app } from "../../schema/app.js";

/**
 * Returns the display name for a chat.
 *
 * Priority:
 * 1. Explicit `chatName` if set
 * 2. Comma-joined names of *other* members (excluding current user)
 * 3. Chat start date if you're the only member (DD Mon YYYY HH:MM)
 * 4. "Chat" while loading
 */
export function useChatDisplayName(chatId: string, chatName?: string): string {
  const session = useSession();
  const userId = session?.user_id ?? null;

  const members = useAll(app.chatMembers.where({ chatId })) ?? [];
  const allProfiles = useAll(app.profiles) ?? [];
  const messages =
    useAll(app.messages.where({ chatId }).orderBy("createdAt", "asc").limit(1)) ?? [];

  if (chatName) return chatName;

  if (members.length === 0) return "Chat";

  const firstMessage = messages[0];
  const dateSuffix = firstMessage ? ` · ${formatChatDate(firstMessage.createdAt)}` : "";

  const memberUserIds = new Set(members.map((m) => m.userId));
  const otherNames = allProfiles
    .filter((p) => memberUserIds.has(p.userId) && p.userId !== userId)
    .map((p) => p.name);

  if (otherNames.length > 0) return "Chat with " + otherNames.join(", ") + dateSuffix;

  // Solo chat — just the date
  return firstMessage ? formatChatDate(firstMessage.createdAt) : "Chat";
}

function formatChatDate(date: Date): string {
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  const d = date.getDate().toString().padStart(2, "0");
  const mon = months[date.getMonth()];
  const y = date.getFullYear();
  const h = date.getHours().toString().padStart(2, "0");
  const m = date.getMinutes().toString().padStart(2, "0");
  return `${d} ${mon} ${y} ${h}:${m}`;
}
