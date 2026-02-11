import { useSuspenseCoState } from "jazz-tools/react";
import { ChatAccount } from "@/schema";

export function CollaboratorBadge({
  accountId,
  color,
}: {
  accountId: string;
  color: string;
}) {
  const user = useSuspenseCoState(ChatAccount, accountId, {
    resolve: { profile: true },
  });

  return (
    <span className="flex items-center gap-2">
      <span
        className="inline-block h-3 w-3 rounded-full border border-stone-200"
        style={{ backgroundColor: color }}
      />
      <span>{user.profile.name}</span>
    </span>
  );
}
