import { schema as s } from "jazz-tools";
import type { JazzContext } from "jazz-tools/backend";

// #region invite-schema
const schema = {
  chats: s.table({
    joinCode: s.string().optional(),
  }),
  chatMembers: s.table({
    chatId: s.ref("chats"),
    user_id: s.string(),
    joinCode: s.string().optional(),
  }),
};
// #endregion invite-schema

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

// #region invite-permissions
s.definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  policy.chats.allowRead.where((chat) =>
    policy.chatMembers.exists.where({ chatId: chat.id, user_id: session.user_id }),
  );
  policy.chats.allowInsert.always();

  // Users can read their own membership row; chat creators can read every
  // member of their chats. Explicit because the row carries the join code,
  // and we don't want it leaking through other members' rows.
  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { user_id: session.user_id },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
    ]),
  );

  // The creator can insert their own membership in their own chat. Everyone
  // else must come through the server route, which writes with backend
  // privileges.
  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { user_id: session.user_id },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
    ]),
  );

  // Users can leave; chat creators can remove any member.
  policy.chatMembers.allowDelete.where((member) =>
    anyOf([
      { user_id: session.user_id },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
    ]),
  );
});
// #endregion invite-permissions

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
    .insert(app.chatMembers, { chatId, user_id: userId, joinCode: code })
    .wait({ tier: "edge" });

  return Response.json({ ok: true });
}
// #endregion invite-redeem-route
