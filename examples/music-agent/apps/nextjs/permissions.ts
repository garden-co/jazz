import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema";

/**
 * A workspace belongs to one Jazz account. The browser may start
 * conversations, write the user's own turns and attach audio to them. Only
 * the server (backend authority) writes assistant turns, tool calls and the
 * seeded booking data, so a client can never forge an agent reply.
 */
export default definePermissions(app, ({ policy, session, allOf, anyOf, allowedTo }) => {
  for (const table of [
    policy.better_auth_user,
    policy.better_auth_session,
    policy.better_auth_account,
    policy.better_auth_verification,
    policy.better_auth_jwks,
  ]) {
    table.allowRead.never();
    table.allowInsert.never();
    table.allowUpdate.never();
    table.allowDelete.never();
  }

  const mine = { ownerAccount: session.user.account };
  policy.profiles.allowRead.where({ accountId: session.user.account });
  policy.artists.allowRead.where(mine);
  policy.songs.allowRead.where(mine);
  policy.venues.allowRead.where(mine);
  policy.calendarEvents.allowRead.where(mine);

  // A conversation may only name an artist from the same workspace: the agent
  // reads that artist's data with backend authority.
  policy.conversations.allowRead.where(mine);
  policy.conversations.allowInsert.where((conversation) =>
    allOf([mine, policy.artists.exists.where({ id: conversation.artistId, ...mine })]),
  );
  policy.conversations.allowUpdate
    .whereOld(mine)
    .whereNew((conversation) =>
      allOf([mine, policy.artists.exists.where({ id: conversation.artistId, ...mine })]),
    );
  policy.conversations.allowDelete.where(mine);

  policy.turns.allowRead.where(allowedTo.read("conversation"));
  policy.toolCalls.allowRead.where(allowedTo.read("conversation"));
  policy.attachments.allowRead.where(allowedTo.read("conversation"));

  policy.turns.allowInsert.where((turn) =>
    allOf([
      { role: "user", status: "complete" },
      policy.conversations.exists.where({ id: turn.conversationId, ...mine }),
      // A turn continues the conversation it belongs to: its parent, if any,
      // is a turn of the same conversation.
      anyOf([
        { parentId: null },
        policy.turns.exists.where({ id: turn.parentId, conversationId: turn.conversationId }),
      ]),
    ]),
  );
  policy.attachments.allowInsert.where((attachment) =>
    allOf([
      policy.conversations.exists.where({ id: attachment.conversationId, ...mine }),
      policy.turns.exists.where({
        id: attachment.turnId,
        conversationId: attachment.conversationId,
        role: "user",
      }),
    ]),
  );
});
