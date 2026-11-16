import { schema as s } from "jazz-tools";
import { ACTIVITY_KINDS, TASK_STATUSES } from "./app-extras";

export const schema = s.defineSchema({
  crew: s.table(
    {
      account: s.uuid(),
      name: s.string(),
    },
    {
      memberships: s.reverse("showCrew", "crew"),
      assignedTasks: s.reverse("tasks", "assignee"),
    },
  ),
  shows: s.table(
    {
      name: s.string(),
      venue: s.string(),
      /** Local date of the show, as an ISO date (YYYY-MM-DD). */
      date: s.string(),
      /** Local doors time at the venue (HH:MM). */
      doors: s.string(),
      /** The crew chief: the account that created and manages the show. */
      chiefAccount: s.uuid(),
    },
    {
      crew: s.reverse("showCrew", "show"),
      tasks: s.reverse("tasks", "show"),
      activity: s.reverse("activity", "show"),
      invites: s.reverse("showInvites", "show"),
    },
  ),
  showCrew: s.table(
    {
      showId: s.uuid(),
      crewId: s.uuid(),
      account: s.uuid(),
      role: s.enum("chief", "crew"),
      /**
       * The invite code a crew member joins with, checked by permissions.
       * The app clears it once the server accepts the membership, because
       * the rest of the crew can read membership rows.
       */
      inviteCode: s.string().optional(),
    },
    {
      show: s.rel("shows", "showId"),
      crew: s.rel("crew", "crewId"),
    },
  ),
  showInvites: s.table(
    {
      showId: s.uuid(),
      code: s.string(),
    },
    { show: s.rel("shows", "showId") },
  ),
  tasks: s.table(
    {
      showId: s.uuid(),
      title: s.string(),
      status: s.enum(...TASK_STATUSES),
      assigneeId: s.uuid().optional(),
      // Concurrent edits to a task's notes merge instead of the last one
      // winning.
      notes: s.string().optional().merge("text"),
      /** Sort key inside a column. Lower values come first. */
      rank: s.float(),
    },
    {
      show: s.rel("shows", "showId"),
      assignee: s.rel("crew", "assigneeId"),
      comments: s.reverse("comments", "task"),
      activity: s.reverse("activity", "task"),
    },
  ),
  comments: s.table(
    {
      taskId: s.uuid(),
      authorId: s.uuid(),
      body: s.string(),
    },
    {
      task: s.rel("tasks", "taskId"),
      author: s.rel("crew", "authorId"),
    },
  ),
  activity: s.table(
    {
      showId: s.uuid(),
      taskId: s.uuid(),
      actorId: s.uuid(),
      kind: s.enum(...ACTIVITY_KINDS),
      /** Short human-readable detail, such as the column a task moved to. */
      detail: s.string().optional(),
    },
    {
      show: s.rel("shows", "showId"),
      task: s.rel("tasks", "taskId"),
      actor: s.rel("crew", "actorId"),
    },
  ),
  checklistItems: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      ownerAccount: s.uuid(),
    },
    {},
  ),
});

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
const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isChief = (showId: s.Uuid) => exists.shows({ id: showId, chiefAccount: session.user });

// The chief counts as crew even before their own showCrew row syncs.
const isCrew = (showId: s.Uuid) =>
  anyOf(exists.showCrew({ showId, account: session.user }), isChief(showId));

const isMyProfile = (crewId: s.Uuid) => exists.crew({ id: crewId, account: session.user });

// A task is assigned to nobody or to someone on the show's crew.
const assigneeIsCrew = (showId: s.Uuid, assigneeId: s.Uuid) =>
  anyOf({ assigneeId: null }, exists.showCrew({ showId, crewId: assigneeId }));

const mine = { account: session.user };

const iAmChief = { chiefAccount: session.user };

const ownedByMe = { ownerAccount: session.user };

export const permissions = definePermissions({
  // Activity is an append-only log per show.
  activity: {
    read: (entry) => isCrew(entry.showId),
    insert: (entry) => allOf(isCrew(entry.showId), isMyProfile(entry.actorId)),
  },
  // A personal checklist.
  checklistItems: {
    read: ownedByMe,
    insert: ownedByMe,
    update: ownedByMe,
    delete: ownedByMe,
  },
  // Comments follow their task; authors delete their own.
  comments: {
    read: (comment) => allowedTo.read(comment.task),
    insert: (comment) => allOf(allowedTo.read(comment.task), isMyProfile(comment.authorId)),
    delete: (comment) => isMyProfile(comment.authorId),
  },
  // Crew profiles: names are shared, only you edit yours.
  crew: {
    read: true,
    insert: mine,
    update: mine,
  },
  showCrew: {
    read: (member) => isCrew(member.showId),
    insert: (member) =>
      anyOf(
        // The chief adds themselves when creating the show.
        allOf(
          { account: session.user, role: "chief" },
          isChief(member.showId),
          isMyProfile(member.crewId),
        ),
        // Anyone holding the current invite code can join as crew.
        allOf(
          { account: session.user, role: "crew" },
          isMyProfile(member.crewId),
          exists.showInvites({ showId: member.showId, code: member.inviteCode }),
        ),
      ),
    // Two updates, each keeping the row on the same show and profile:
    // - once the server has accepted a membership, the joiner clears its invite
    //   code, so other crew can't read the code;
    // - the chief rewrites their own membership when a repair from another tab
    //   lands on the same row (it has a stable id).
    updateIf: mine,
    updateTo: (member, old) =>
      allOf(
        { account: session.user, inviteCode: null },
        isMyProfile(member.crewId),
        anyOf(
          allOf(
            { role: "crew" },
            exists.showCrew({ id: member.id, showId: member.showId, role: "crew" }),
          ),
          allOf({ role: "chief" }, isChief(member.showId), { showId: old.showId }),
        ),
      ),
    // Members leave on their own; the chief can remove anyone.
    delete: (member) => anyOf(mine, isChief(member.showId)),
  },
  // Invite codes are bearer secrets: only the chief reads or rotates them. A
  // joiner's membership carries the code until they clear it (see showCrew).
  showInvites: {
    read: (invite) => isChief(invite.showId),
    insert: (invite) => isChief(invite.showId),
    // A repair of the first invite from another tab rewrites the same row.
    updateIf: (invite) => isChief(invite.showId),
    updateTo: (invite, old) => allOf(isChief(invite.showId), { showId: old.showId }),
    delete: (invite) => isChief(invite.showId),
  },
  shows: {
    read: (show) => isCrew(show.id),
    insert: iAmChief,
    update: iAmChief,
    delete: iAmChief,
  },
  // Any crew member edits tasks; the chief or the task's creator deletes them.
  tasks: {
    read: (task) => isCrew(task.showId),
    insert: (task) => allOf(isCrew(task.showId), assigneeIsCrew(task.showId, task.assigneeId)),
    updateIf: (task) => isCrew(task.showId),
    updateTo: (task) => allOf(isCrew(task.showId), assigneeIsCrew(task.showId, task.assigneeId)),
    delete: (task) =>
      anyOf(
        isChief(task.showId),
        allOf({ "$createdBy.account": session.user }, isCrew(task.showId)),
      ),
  },
});
