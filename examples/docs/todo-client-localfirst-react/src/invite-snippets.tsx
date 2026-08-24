import { useEffect, useRef } from "react";
import { schema as s } from "jazz-tools";
import { useDb } from "jazz-tools/react";

const schema = {
  chats: s.table({}),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    user_id: s.string(),
    inviteId: s.string().optional(),
  }),
  chatInvites: s.table({
    chatId: s.ref("chats"),
    code: s.string(),
    singleUse: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

// Stand-in for the reader's router. With React Router, this would be
// `const navigate = useNavigate()` inside the component.
function navigate(to: string) {
  window.location.hash = to;
}

// #region invite-create-link
export function createInviteLink(
  db: ReturnType<typeof useDb>,
  userId: string,
  { singleUse = false }: { singleUse?: boolean } = {},
): string {
  const joinCode = crypto.randomUUID();

  const { value: chat } = db.insert(app.chats, {});

  db.insert(app.chatMembers, { chatId: chat.id, user_id: userId });
  db.insert(app.chatInvites, { chatId: chat.id, code: joinCode, singleUse });

  return `${window.location.origin}/#/invite/${chat.id}/${joinCode}`;
}
// #endregion invite-create-link

// #region invite-revoke
export function revokeMember(db: ReturnType<typeof useDb>, memberId: string) {
  db.delete(app.chatMembers, memberId);
}
// #endregion invite-revoke

// #region invite-handler-primary
export function InviteHandler({ chatId, code }: { chatId: string; code: string }) {
  const handled = useRef(false);

  useEffect(() => {
    if (handled.current) return;
    handled.current = true;

    fetch("/api/invite/redeem", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chatId, code }),
    })
      .then((res) => {
        if (!res.ok) throw new Error(`redeem failed: ${res.status}`);
        navigate(`/chat/${chatId}`);
      })
      .catch((err) => {
        console.error("failed to join", err);
        handled.current = false;
      });
  }, [chatId, code]);

  return <p>Joining…</p>;
}
// #endregion invite-handler-primary
