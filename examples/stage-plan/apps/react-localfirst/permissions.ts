import { schema as s, type RowRefValue } from "jazz-tools";
import { app } from "./schema.js";

/**
 * Who can do what in StagePlan:
 *
 * - The crew chief (the account that created a show) manages the show, its
 *   invite link and its crew.
 * - Crew members (anyone with a showCrew row) see the show and can add, edit
 *   and move its tasks, comment and log activity.
 * - Everyone else sees nothing of the show: not the show, its tasks,
 *   comments, activity or crew list.
 * - Checklist items are private to their owner.
 */
export default s.definePermissions(app, ({ policy, session, anyOf, allOf, allowedTo }) => {
  const me = session.user.account;

  const isChief = (showId: RowRefValue) =>
    policy.shows.exists.where({ id: showId, chiefAccount: me });
  // The chief counts as crew even before their own showCrew row syncs.
  const isCrew = (showId: RowRefValue) =>
    anyOf([policy.showCrew.exists.where({ showId, account: me }), isChief(showId)]);
  const isMyProfile = (crewId: RowRefValue) =>
    policy.crew.exists.where({ id: crewId, account: me });

  // --- crew profiles: names are shared, only you edit yours ---
  policy.crew.allowRead.always();
  policy.crew.allowInsert.where({ account: me });
  policy.crew.allowUpdate.whereOld({ account: me }).whereNew({ account: me });

  // --- shows ---
  policy.shows.allowRead.where((show) => isCrew(show.id));
  policy.shows.allowInsert.where({ chiefAccount: me });
  policy.shows.allowUpdate.whereOld({ chiefAccount: me }).whereNew({ chiefAccount: me });
  policy.shows.allowDelete.where({ chiefAccount: me });

  // --- showCrew memberships ---
  policy.showCrew.allowRead.where((member) => isCrew(member.showId));
  policy.showCrew.allowInsert.where((member) =>
    anyOf([
      // The chief adds themselves when creating the show.
      allOf([{ account: me, role: "chief" }, isChief(member.showId), isMyProfile(member.crewId)]),
      // Anyone holding the current invite code can join as crew.
      allOf([
        { account: me, role: "crew" },
        isMyProfile(member.crewId),
        policy.showInvites.exists.where({ showId: member.showId, code: member.inviteCode }),
      ]),
    ]),
  );
  // Once the server has accepted a membership, the joiner clears the invite
  // code from it, so other crew can't read the code. That's the only change
  // allowed: same row, same show, same profile, still crew.
  policy.showCrew.allowUpdate
    .whereOld({ account: me, role: "crew" })
    .whereNew((member) =>
      allOf([
        { account: me, role: "crew", inviteCode: { isNull: true } },
        isMyProfile(member.crewId),
        policy.showCrew.exists.where({ id: member.id, showId: member.showId }),
      ]),
    );
  // Members leave on their own; the chief can remove anyone.
  policy.showCrew.allowDelete.where((member) => anyOf([{ account: me }, isChief(member.showId)]));

  // --- invite codes are bearer secrets: only the chief reads or rotates them.
  // A joiner's membership carries the code until they clear it (see above). ---
  policy.showInvites.allowRead.where((invite) => isChief(invite.showId));
  policy.showInvites.allowInsert.where((invite) => isChief(invite.showId));
  policy.showInvites.allowDelete.where((invite) => isChief(invite.showId));

  // --- tasks: any crew member edits; the chief or the task's creator deletes ---
  // A task is assigned to nobody or to someone on the show's crew.
  const assigneeIsCrew = (task: { showId: RowRefValue; assigneeId: RowRefValue }) =>
    anyOf([
      { assigneeId: { isNull: true } },
      policy.showCrew.exists.where({ showId: task.showId, crewId: task.assigneeId }),
    ]);
  policy.tasks.allowRead.where((task) => isCrew(task.showId));
  policy.tasks.allowInsert.where((task) => allOf([isCrew(task.showId), assigneeIsCrew(task)]));
  policy.tasks.allowUpdate
    .whereOld((task) => isCrew(task.showId))
    .whereNew((task) => allOf([isCrew(task.showId), assigneeIsCrew(task)]));
  policy.tasks.allowDelete.where((task) =>
    anyOf([isChief(task.showId), allOf([{ "$createdBy.account": me }, isCrew(task.showId)])]),
  );

  // --- comments follow their task; authors delete their own ---
  policy.comments.allowRead.where(allowedTo.read("task"));
  policy.comments.allowInsert.where((comment) =>
    allOf([allowedTo.read("task"), isMyProfile(comment.authorId)]),
  );
  policy.comments.allowDelete.where((comment) => isMyProfile(comment.authorId));

  // --- activity is an append-only log per show (W1's row-dependent policy) ---
  policy.activity.allowRead.where((entry) => isCrew(entry.showId));
  policy.activity.allowInsert.where((entry) =>
    allOf([isCrew(entry.showId), isMyProfile(entry.actorId)]),
  );

  // --- personal checklist ---
  policy.checklistItems.allowRead.where({ ownerAccount: me });
  policy.checklistItems.allowInsert.where({ ownerAccount: me });
  policy.checklistItems.allowUpdate.whereOld({ ownerAccount: me }).whereNew({ ownerAccount: me });
  policy.checklistItems.allowDelete.where({ ownerAccount: me });
});
