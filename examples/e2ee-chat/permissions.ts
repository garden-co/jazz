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
  policy.__e2ee_spaces.allowRead.where(authenticated);
  policy.__e2ee_spaces.allowInsert.where((space) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      // Account correlation comes first: it is the shared declared-reference join.
      policy.chatOwners.existsIncludingCreated.where({
        accountId: space.accountId,
        chatId: space.identifier,
      }),
    ]),
  );
  policy.__e2ee_space_grants.allowRead.where(authenticated);
  policy.__e2ee_space_grants.allowInsert.where((grant) =>
    allOf([
      { authorAccountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_spaces.existsIncludingCreated.where({
        id: grant.spaceId,
        accountId: session.user.account,
      }),
    ]),
  );
  policy.__e2ee_space_successors.allowRead.where(authenticated);
  policy.__e2ee_space_successors.allowInsert.where((successor) =>
    allOf([
      { authorAccountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_spaces.exists.where({ id: successor.spaceId, accountId: session.user.account }),
    ]),
  );
  policy.__e2ee_space_deliveries.allowRead.where(authenticated);
  policy.__e2ee_space_deliveries.allowInsert.where((delivery) =>
    allOf([
      { senderAccountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_spaces.exists.where({ id: delivery.spaceId, accountId: session.user.account }),
    ]),
  );
});
