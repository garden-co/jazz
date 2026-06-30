import { useEffect, useMemo, useRef } from "react";
import { useDb, useSession } from "jazz-tools/react";
import { useRouter } from "@/hooks/useRouter";
import { useMyProfile } from "@/hooks/useMyProfile";
import { app } from "../../schema.js";
import { type DurabilityTier } from "jazz-tools";

interface InviteHandlerProps {
  chatId: string;
  code: string;
}

export function InviteHandler({ chatId, code }: InviteHandlerProps) {
  const db = useDb();
  const session = useSession();
  const { navigate } = useRouter();
  const handled = useRef(false);
  const sharedWriteOptions: { tier: DurabilityTier } = useMemo(
    () => ({ tier: db.getConfig().serverUrl ? "edge" : "local" }),
    [db],
  );

  const userId = session?.user_id ?? null;
  const myProfile = useMyProfile();

  useEffect(() => {
    if (handled.current || !userId || !myProfile) return;
    handled.current = true;

    void db
      .insert(app.chatMembers, {
        chatId,
        userId,
        joinCode: code,
      })
      .wait(sharedWriteOptions)
      .then(() => {
        navigate(`/#/chat/${chatId}`);
      })
      .catch((error) => {
        console.error("failed to accept invite", error);
        handled.current = false;
      });
  }, [db, userId, myProfile, chatId, code, sharedWriteOptions, navigate]);

  return (
    <div id="joining-chat" className="p-8 text-center text-muted-foreground italic">
      Joining chat...
    </div>
  );
}
