import { schema as s, userIdentity } from "jazz-tools";
import type { JazzContext } from "jazz-tools/backend";

// #region invite-schema
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
// #endregion invite-schema

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

// #region invite-permissions
s.definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  policy.chats.allowRead.where((chat) =>
    policy.chatMembers.exists.where({ chatId: chat.id, user_id: session.user }),
  );
  policy.chats.allowInsert.always();

  // Users can read their own membership row; chat creators can read every
  // member of their chats.
  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { user_id: session.user },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user }),
    ]),
  );

  // The creator can insert their own membership in their own chat. Everyone
  // else must come through the server route, which writes with backend
  // privileges.
  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { user_id: session.user },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user }),
    ]),
  );

  // Users can leave; chat creators can remove any member.
  policy.chatMembers.allowDelete.where((member) =>
    anyOf([
      { user_id: session.user },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user }),
    ]),
  );

  // Invite codes are bearer capabilities. They never sync back down to a client.
  policy.chatInvites.allowRead.never();
  policy.chatInvites.allowInsert.where((invite) =>
    policy.chats.exists.where({ id: invite.chatId, $createdBy: session.user }),
  );
  policy.chatInvites.allowDelete.where((invite) =>
    policy.chats.exists.where({ id: invite.chatId, $createdBy: session.user }),
  );
});
// #endregion invite-permissions

declare const context: JazzContext;

// Supplied by your authentication middleware after it has verified the caller.
type AuthenticatedRequest = Request & {
  session: {
    issuer: string;
    user_id: string;
    authMode: "external" | "local-first";
    claims: Record<string, unknown>;
  };
};

// #region invite-redeem-route
export async function POST(req: AuthenticatedRequest): Promise<Response> {
  const { session } = req;
  const user = userIdentity(session.issuer, session.user_id);

  const { chatId, code } = (await req.json()) as { chatId: string; code: string };

  // This handle has backend permissions but attributes writes to the authenticated session.
  const backendDb = context.withAttributionForSession(session, app);
  const result = await backendDb.exclusiveTransaction(async (tx) => {
    // Checking membership first keeps re-opening a successfully redeemed link idempotent,
    // even after a single-use invite has been consumed.
    const existing = await tx.one(app.chatMembers.where({ chatId, user_id: user }));
    if (existing) return "already-member" as const;

    const invite = await tx.one(app.chatInvites.where({ chatId, code }));
    if (!invite) return "invalid" as const;

    tx.insert(app.chatMembers, { chatId, user_id: user, inviteId: invite.id });
    if (invite.singleUse) tx.delete(app.chatInvites, invite.id);
    return "joined" as const;
  });

  // Exclusive transactions settle at the authority, so wait() takes no tier.
  await result.wait();
  if (result.value === "invalid") return new Response("invalid invite", { status: 400 });

  return Response.json({ ok: true });
}
// #endregion invite-redeem-route
