import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy, session, allOf, anyOf }) => {
  const authenticated = session.where({ authMode: { in: ["local-first", "external"] } });
  policy.chats.allowRead.where((chat) =>
    anyOf([
      { ownerId: session.user.account },
      policy.chatMembers.exists.where({ chatId: chat.id, accountId: session.user.account }),
    ]),
  );
  policy.chats.allowInsert.where({
    ownerId: session.user.account,
    "$createdBy.account": session.user.account,
  });
  // No owner mutation: accepted space administration stays tied to the original owner.
  // Immutable ownership binding supplies declared references for managed root authorization.
  policy.chatOwners.allowRead.where(authenticated);
  policy.chatOwners.allowInsert.where((binding) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      policy.chats.existsIncludingCreated.where({
        id: binding.chatId,
        ownerId: binding.accountId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  policy.chatMembers.allowRead.where((member) =>
    anyOf([
      { accountId: session.user.account },
      policy.chats.exists.where({ id: member.chatId, ownerId: session.user.account }),
    ]),
  );
  policy.chatMembers.allowInsert.where((member) =>
    policy.chats.existsIncludingCreated.where({ id: member.chatId, ownerId: session.user.account }),
  );
  policy.chatMembers.allowUpdate
    .whereOld((member) =>
      policy.chats.exists.where({ id: member.chatId, ownerId: session.user.account }),
    )
    .whereNew((member) =>
      allOf([
        policy.chats.exists.where({ id: member.chatId, ownerId: session.user.account }),
        policy.chatMembers.exists.where({
          id: member.id,
          chatId: member.chatId,
          accountId: member.accountId,
        }),
      ]),
    );
  policy.messages.allowRead.where((message) =>
    anyOf([
      policy.chats.exists.where({ id: message.chatId, ownerId: session.user.account }),
      policy.chatMembers.exists.where({ chatId: message.chatId, accountId: session.user.account }),
    ]),
  );
  policy.messages.allowInsert.where((message) =>
    allOf([
      { senderId: session.user.account, "$createdBy.account": session.user.account },
      anyOf([
        policy.chats.existsIncludingCreated.where({
          id: message.chatId,
          ownerId: session.user.account,
        }),
        policy.chatMembers.existsIncludingCreated.where({
          chatId: message.chatId,
          accountId: session.user.account,
        }),
      ]),
    ]),
  );

  // Device identity/approval policies are package-owned; do not replace them.
  // Authenticated nonmembers can see control metadata, but not chat content.
  policy.messages.space.allowRead.where(authenticated);
  policy.messages.space.allowInsert.where((space) =>
    policy.chatOwners.existsIncludingCreated.where({
      accountId: space.creatorAccountId,
      chatId: space.identifier,
    }),
  );
  policy.messages.space.allowUpdate.where((space) =>
    policy.chatOwners.existsIncludingCreated.where({
      chatId: space.identifier,
      accountId: session.user.account,
    }),
  );
});
