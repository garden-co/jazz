import { schema as s } from "jazz-tools";
import type { JazzContext } from "jazz-tools/backend";

// #region invite-schema
const schema = {
  chats: s.table({
    isPublic: s.boolean(),
    createdBy: s.string(),
    joinCode: s.string().optional(),
  }),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    userId: s.string(),
    joinCode: s.string().optional(),
  }),
};
// #endregion invite-schema

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export const claimsApp: s.App<AppSchema> = s.defineApp(schema);

// #region invite-permissions
s.definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  policy.chats.allowRead.where((chat) =>
    policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id }),
  );
  policy.chats.allowInsert.where({ createdBy: session.user_id });

  // Users can read their own membership row; chat creators can read every
  // member of their chats. Explicit because the row carries the join code,
  // and we don't want it leaking through other members' rows.
  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { userId: session.user_id },
      policy.chats.exists.where({ id: member.chatId, createdBy: session.user_id }),
    ]),
  );

  // The creator can insert their own membership in their own chat. Everyone
  // else must come through the server route, which writes with backend
  // privileges.
  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { userId: session.user_id },
      policy.chats.exists.where({ id: member.chatId, createdBy: session.user_id }),
    ]),
  );

  // Users can leave; chat creators can remove any member.
  policy.chatMembers.allowDelete.where((member) =>
    anyOf([
      { userId: session.user_id },
      policy.chats.exists.where({ id: member.chatId, createdBy: session.user_id }),
    ]),
  );
});
// #endregion invite-permissions

// #region invite-permissions-jwt-claim
s.definePermissions(claimsApp, ({ policy, allOf, anyOf, session }) => {
  policy.chats.allowRead.where((chat) =>
    anyOf([
      policy.chatMembers.exists.where({ chatId: chat.id, userId: session.user_id }),
      { joinCode: session["claims.join_code"] },
    ]),
  );

  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { userId: session.user_id },
      policy.chats.exists.where({ id: member.chatId, createdBy: session.user_id }),
    ]),
  );

  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { userId: session.user_id },
      anyOf([
        policy.chats.exists.where({ id: member.chatId, createdBy: session.user_id }),
        policy.chats.exists.where({
          id: member.chatId,
          joinCode: session["claims.join_code"],
        }),
      ]),
    ]),
  );
});
// #endregion invite-permissions-jwt-claim

declare const context: JazzContext;

// Supplied by the reader's auth middleware.
type AuthenticatedRequest = Request & { session: { user_id: string | null } };

// #region invite-redeem-route
export async function POST(req: AuthenticatedRequest): Promise<Response> {
  const userId = req.session.user_id;
  if (!userId) return new Response("unauthenticated", { status: 401 });

  const { chatId, code } = (await req.json()) as { chatId: string; code: string };

  const chat = await context.asBackend(app).one(app.chats.where({ id: chatId }));
  if (!chat || chat.joinCode !== code) {
    return new Response("invalid invite", { status: 400 });
  }

  await context
    .withAttribution(userId, app)
    .insert(app.chatMembers, { chatId, userId, joinCode: code })
    .wait({ tier: "edge" });

  return Response.json({ ok: true });
}
// #endregion invite-redeem-route
