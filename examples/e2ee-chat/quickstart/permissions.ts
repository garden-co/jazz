import { definePermissions } from "jazz-tools/permissions";
import { app } from "./schema.js";

export default definePermissions(app, ({ policy: p, session }) => {
  p.chats.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
  p.chats.allowInsert.where({
    ownerId: session.user.account,
    "$createdBy.account": session.user.account,
  });
  // Omitted chat update/delete rules keep ownership immutable.
  p.messages.fromEncryptionSpace({ write: { "$createdBy.account": session.user.account } });
  p.messages.space.allowRead.always();
  p.messages.space.allowInsert.where((space) =>
    p.chats.existsIncludingCreated.where({
      ownerId: space.creatorAccountId,
      id: space.identifier,
    }),
  );
  p.messages.space.allowUpdate.where((space) =>
    p.chats.existsIncludingCreated.where({
      id: space.identifier,
      ownerId: session.user.account,
    }),
  );
});
