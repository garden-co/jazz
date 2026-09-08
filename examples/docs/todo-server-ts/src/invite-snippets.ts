import { schema as s } from "jazz-tools";
import type { JazzClient } from "jazz-tools/backend";

// #region invite-schema
const schema = {
  chats: s.table({}),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    user_id: s.uuid(),
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
    policy.chatMembers.exists.where({ chatId: chat.id, user_id: session.user.account }),
  );
  policy.chats.allowInsert.always();

  // Users can read their own membership row; chat creators can read every
  // member of their chats.
  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { user_id: session.user.account },
      policy.chats.exists.where({ id: member.chatId, "$createdBy.account": session.user.account }),
    ]),
  );

  // The creator can insert their own membership in their own chat. Everyone
  // else must come through the server route, which writes with backend
  // privileges.
  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { user_id: session.user.account },
      policy.chats.exists.where({ id: member.chatId, "$createdBy.account": session.user.account }),
    ]),
  );

  // Users can leave; chat creators can remove any member.
  policy.chatMembers.allowDelete.where((member) =>
    anyOf([
      { user_id: session.user.account },
      policy.chats.exists.where({ id: member.chatId, "$createdBy.account": session.user.account }),
    ]),
  );

  // Invite codes are bearer capabilities. They never sync back down to a client.
  policy.chatInvites.allowRead.never();
  policy.chatInvites.allowInsert.where((invite) =>
    policy.chats.exists.where({ id: invite.chatId, "$createdBy.account": session.user.account }),
  );
  policy.chatInvites.allowDelete.where((invite) =>
    policy.chats.exists.where({ id: invite.chatId, "$createdBy.account": session.user.account }),
  );
});
// #endregion invite-permissions

declare const client: JazzClient;

// #region invite-redeem-route
export async function POST(req: Request): Promise<Response> {
  const requester = await client.forRequest(req);
  const user = requester.getAuthState().session?.user.account;
  if (!user) return new Response("Account required", { status: 401 });

  const { chatId, code } = (await req.json()) as { chatId: string; code: string };

  // Preserve the verified caller as author while using backend permissions.
  const backendDb = await client.withAttributionForRequest(req);
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
