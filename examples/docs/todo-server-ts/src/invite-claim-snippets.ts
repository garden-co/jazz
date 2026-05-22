import { schema as s } from "jazz-tools";

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

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

// #region invite-permissions-jwt-claim
s.definePermissions(app, ({ policy, allOf, anyOf, session }) => {
  policy.chats.allowRead.where((chat) =>
    anyOf([
      policy.chatMembers.exists.where({ chatId: chat.id, user_id: session.user_id }),
      { joinCode: session["claims.join_code"] },
    ]),
  );
  policy.chats.allowInsert.always();

  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { user_id: session.user_id },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
    ]),
  );

  policy.chatMembers.allowInsert.where((member) =>
    allOf([
      { user_id: session.user_id },
      anyOf([
        policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
        policy.chats.exists.where({
          id: member.chatId,
          joinCode: session["claims.join_code"],
        }),
      ]),
    ]),
  );

  policy.chatMembers.allowDelete.where((member) =>
    anyOf([
      { user_id: session.user_id },
      policy.chats.exists.where({ id: member.chatId, $createdBy: session.user_id }),
    ]),
  );
});
// #endregion invite-permissions-jwt-claim
