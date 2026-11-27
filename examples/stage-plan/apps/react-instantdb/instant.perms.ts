import type { InstantRules } from "@instantdb/react";

/**
 * Who can do what in StagePlan (the same rules as the Jazz app):
 *
 * - The crew chief (the account that created a show) manages the show, its
 *   invite link and its crew.
 * - Crew members (anyone with a showCrew row) see the show and can add, edit
 *   and move its tasks, comment and log activity.
 * - Everyone else sees nothing of the show: not the show, its tasks,
 *   comments, activity or crew list.
 * - Checklist items are private to their owner.
 *
 * Rows of a show reach it through their `show` link; `data.ref` follows it.
 */
const signedIn = "auth.id != null";
// For a row linked to a show:
const isChiefOf = (path: string) => `auth.id in data.ref('${path}.chiefAccount')`;
const isCrewOf = (path: string) =>
  `(${isChiefOf(path)} || auth.id in data.ref('${path}.crew.account'))`;

const rules = {
  // No new attributes or namespaces from clients.
  attrs: { allow: { $default: "false" } },
  $default: { allow: { $default: "false" } },

  // --- crew profiles: names are shared, only you edit yours ---
  crew: {
    allow: {
      view: signedIn,
      create: "isMine",
      update: "isMine && newData.account == auth.id",
      delete: "false",
    },
    bind: { isMine: `${signedIn} && data.account == auth.id` },
  },

  // --- shows ---
  shows: {
    allow: {
      // A joiner holding the invite code sees the show, so their membership
      // can link to it (Instant checks the linked row's view rule).
      view: "isChief || auth.id in data.ref('crew.account') || hasInvite",
      create: "isChief",
      update: "isChief && newData.chiefAccount == auth.id",
      delete: "isChief",
    },
    bind: {
      isChief: `${signedIn} && data.chiefAccount == auth.id`,
      hasInvite: `${signedIn} && ruleParams.inviteCode in data.ref('invites.code')`,
    },
  },

  // --- showCrew memberships ---
  showCrew: {
    allow: {
      view: isCrewOf("show"),
      // The chief adds themselves when creating the show; anyone holding the
      // show's current invite code (passed as a rule param) joins as crew.
      create: `isMine && isMyProfile && (
        (data.role == 'chief' && ${isChiefOf("show")}) ||
        (data.role == 'crew' && ruleParams.inviteCode in data.ref('show.invites.code'))
      )`,
      update: "false",
      // Members leave on their own; the chief can remove anyone.
      delete: `isMine || ${isChiefOf("show")}`,
    },
    bind: {
      isMine: `${signedIn} && data.account == auth.id`,
      isMyProfile: "auth.id in data.ref('crew.account')",
    },
  },

  // --- invite codes are bearer secrets: only the chief reads or rotates them ---
  showInvites: {
    allow: {
      view: isChiefOf("show"),
      create: isChiefOf("show"),
      update: "false",
      delete: isChiefOf("show"),
    },
  },

  // --- tasks: any crew member edits; the chief or the task's creator deletes ---
  // A task is assigned to nobody or to someone on the show's crew.
  tasks: {
    allow: {
      view: isCrewOf("show"),
      create: `${isCrewOf("show")} && data.creatorAccount == auth.id && assigneeIsCrew`,
      update: `${isCrewOf("show")} && newData.creatorAccount == data.creatorAccount && newAssigneeIsCrew`,
      delete: `${isChiefOf("show")} || (data.creatorAccount == auth.id && ${isCrewOf("show")})`,
    },
    bind: {
      assigneeIsCrew: "data.assigneeId == null || data.assigneeId in data.ref('show.crew.crew.id')",
      newAssigneeIsCrew:
        "newData.assigneeId == null || newData.assigneeId in data.ref('show.crew.crew.id')",
    },
  },

  // --- comments follow their task; authors delete their own ---
  comments: {
    allow: {
      view: isCrewOf("task.show"),
      create: `${isCrewOf("task.show")} && isMyComment`,
      update: "false",
      delete: "isMyComment",
    },
    bind: { isMyComment: "auth.id in data.ref('author.account')" },
  },

  // --- activity is an append-only log per show ---
  activity: {
    allow: {
      view: isCrewOf("show"),
      create: `${isCrewOf("show")} && auth.id in data.ref('actor.account')`,
      update: "false",
      delete: "false",
    },
  },

  // --- personal checklist ---
  checklistItems: {
    allow: {
      view: "isMine",
      create: "isMine",
      update: "isMine && newData.ownerAccount == auth.id",
      delete: "isMine",
    },
    bind: { isMine: `${signedIn} && data.ownerAccount == auth.id` },
  },
} satisfies InstantRules;

export default rules;
