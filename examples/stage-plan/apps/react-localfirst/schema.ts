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

const { definePermissions, exists, allowedTo, session, anyOf, allOf } = schema;

const isShowCrew = (showId: s.Uuid) => exists.showCrew({ showId, account: session.user });

const chiefAccountIsMe = { chiefAccount: session.user };

const accountIsMe = { account: session.user };

const ownerAccountIsMe = { ownerAccount: session.user };

const isShowCrew2 = (showId: s.Uuid, crewId: s.Uuid) => exists.showCrew({ showId, crewId });

export const permissions = definePermissions({
  activity: {
    read: (activity) => anyOf(isShowCrew(activity.showId), activity.show.where(chiefAccountIsMe)),
    insert: (activity) =>
      allOf(
        anyOf(isShowCrew(activity.showId), activity.show.where(chiefAccountIsMe)),
        activity.actor.where(accountIsMe),
      ),
  },
  checklistItems: {
    read: ownerAccountIsMe,
    insert: ownerAccountIsMe,
    update: ownerAccountIsMe,
    delete: ownerAccountIsMe,
  },
  comments: {
    read: (comment) => allowedTo.read(comment.task),
    insert: (comment) => allOf(allowedTo.read(comment.task), comment.author.where(accountIsMe)),
    delete: (comment) => comment.author.where(accountIsMe),
  },
  crew: {
    read: true,
    insert: accountIsMe,
    update: accountIsMe,
  },
  showCrew: {
    read: (showCrew) => anyOf(isShowCrew(showCrew.showId), showCrew.show.where(chiefAccountIsMe)),
    insert: (showCrew) =>
      anyOf(
        allOf(
          { account: session.user, role: "chief" },
          showCrew.show.where(chiefAccountIsMe),
          showCrew.crew.where(accountIsMe),
        ),
        allOf(
          { account: session.user, role: "crew" },
          showCrew.crew.where(accountIsMe),
          exists.showInvites({ showId: showCrew.showId, code: showCrew.inviteCode }),
        ),
      ),
    updateIf: accountIsMe,
    updateTo: (showCrew, old) =>
      allOf(
        { account: session.user, inviteCode: null },
        showCrew.crew.where(accountIsMe),
        anyOf(
          allOf(
            { role: "crew" },
            exists.showCrew({ id: showCrew.id, showId: showCrew.showId, role: "crew" }),
          ),
          allOf({ role: "chief" }, showCrew.show.where(chiefAccountIsMe), { showId: old.showId }),
        ),
      ),
    delete: (showCrew) => anyOf(accountIsMe, showCrew.show.where(chiefAccountIsMe)),
  },
  showInvites: {
    read: (showInvite) => showInvite.show.where(chiefAccountIsMe),
    insert: (showInvite) => showInvite.show.where(chiefAccountIsMe),
    updateIf: (showInvite) => showInvite.show.where(chiefAccountIsMe),
    updateTo: (showInvite, old) =>
      allOf(showInvite.show.where(chiefAccountIsMe), { showId: old.showId }),
    delete: (showInvite) => showInvite.show.where(chiefAccountIsMe),
  },
  shows: {
    read: (show) =>
      anyOf(isShowCrew(show.id), exists.shows({ id: show.id, chiefAccount: session.user })),
    insert: chiefAccountIsMe,
    update: chiefAccountIsMe,
    delete: chiefAccountIsMe,
  },
  tasks: {
    read: (task) => anyOf(isShowCrew(task.showId), task.show.where(chiefAccountIsMe)),
    insert: (task) =>
      allOf(
        anyOf(isShowCrew(task.showId), task.show.where(chiefAccountIsMe)),
        anyOf({ assigneeId: null }, isShowCrew2(task.showId, task.assigneeId)),
      ),
    updateIf: (task) => anyOf(isShowCrew(task.showId), task.show.where(chiefAccountIsMe)),
    updateTo: (task) =>
      allOf(
        anyOf(isShowCrew(task.showId), task.show.where(chiefAccountIsMe)),
        anyOf({ assigneeId: null }, isShowCrew2(task.showId, task.assigneeId)),
      ),
    delete: (task) =>
      anyOf(
        task.show.where(chiefAccountIsMe),
        allOf(
          { $createdBy: session.user },
          anyOf(isShowCrew(task.showId), task.show.where(chiefAccountIsMe)),
        ),
      ),
  },
});
