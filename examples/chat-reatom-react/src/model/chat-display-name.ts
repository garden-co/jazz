import { computed, reatomMap, type Computed } from "@reatom/core";
import { jazz } from "@/jazz";
import { allProfilesQuery, getChatMembersQuery, getFirstChatMessageQuery } from "@/model/queries";

const cache = reatomMap<string, Computed<string>>(undefined, "chatDisplayName._cache");

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function formatChatDate(date: number | Date): string {
  const resolved = date instanceof Date ? date : new Date(date);
  const d = resolved.getDate().toString().padStart(2, "0");
  const mon = MONTHS[resolved.getMonth()];
  const y = resolved.getFullYear();
  const h = resolved.getHours().toString().padStart(2, "0");
  const m = resolved.getMinutes().toString().padStart(2, "0");
  return `${d} ${mon} ${y} ${h}:${m}`;
}

export const getChatDisplayName = (chatId: string, chatName?: string) =>
  cache.getOrCreate(`${chatId}|${chatName ?? ""}`, () =>
    computed(() => {
      if (chatName) return chatName;

      const userId = jazz().session?.user_id ?? null;
      const members = getChatMembersQuery(chatId)();
      if (members.length === 0) return "Chat";

      const allProfiles = allProfilesQuery();
      const messages = getFirstChatMessageQuery(chatId)();
      const firstMessage = messages[0];
      const dateSuffix = firstMessage ? ` · ${formatChatDate(firstMessage.createdAt)}` : "";

      const memberUserIds = new Set(members.map((m) => m.userId));
      const otherNames = allProfiles
        .filter((p) => memberUserIds.has(p.userId) && p.userId !== userId)
        .map((p) => p.name);

      if (otherNames.length > 0) {
        return "Chat with " + otherNames.join(", ") + dateSuffix;
      }

      return firstMessage ? formatChatDate(firstMessage.createdAt) : "Chat";
    }, `chatDisplayName#${chatId}`),
  );
