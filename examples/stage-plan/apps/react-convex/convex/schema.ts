import { defineSchema, defineTable } from "convex/server";
import { authTables } from "@convex-dev/auth/server";
import { v } from "convex/values";

/**
 * StagePlan: a crew preparing shows. Same tables as the Jazz app's schema.ts;
 * `account` fields hold the Convex Auth user id. Who may read or write what
 * is checked inside each function (see lib/access.ts).
 */
export const taskStatus = v.union(
  v.literal("todo"),
  v.literal("doing"),
  v.literal("done"),
  v.literal("blocked"),
);

export const activityKind = v.union(
  v.literal("created"),
  v.literal("moved"),
  v.literal("assigned"),
  v.literal("renamed"),
  v.literal("commented"),
  v.literal("deleted"),
);

export default defineSchema({
  ...authTables,
  crew: defineTable({
    account: v.id("users"),
    name: v.string(),
  }).index("by_account", ["account"]),
  shows: defineTable({
    name: v.string(),
    venue: v.string(),
    /** Local date of the show, as an ISO date (YYYY-MM-DD). */
    date: v.string(),
    /** Local doors time at the venue (HH:MM). */
    doors: v.string(),
    /** The crew chief: the account that created and manages the show. */
    chiefAccount: v.id("users"),
  }),
  showCrew: defineTable({
    showId: v.id("shows"),
    crewId: v.id("crew"),
    account: v.id("users"),
    role: v.union(v.literal("chief"), v.literal("crew")),
  })
    .index("by_show_role", ["showId", "role"])
    .index("by_show_account", ["showId", "account"])
    .index("by_show_crew", ["showId", "crewId"])
    .index("by_account", ["account"]),
  showInvites: defineTable({
    showId: v.id("shows"),
    code: v.string(),
  }).index("by_show_code", ["showId", "code"]),
  tasks: defineTable({
    showId: v.id("shows"),
    title: v.string(),
    status: taskStatus,
    assigneeId: v.optional(v.id("crew")),
    notes: v.optional(v.string()),
    /** Sort key inside a column. Lower values come first. */
    rank: v.number(),
    createdBy: v.id("users"),
    updatedAt: v.number(),
  })
    .index("by_show_rank", ["showId", "rank"])
    .index("by_show_assignee", ["showId", "assigneeId"]),
  comments: defineTable({
    taskId: v.id("tasks"),
    authorId: v.id("crew"),
    body: v.string(),
  }).index("by_task", ["taskId"]),
  activity: defineTable({
    showId: v.id("shows"),
    taskId: v.id("tasks"),
    actorId: v.id("crew"),
    kind: activityKind,
    /** Short human-readable detail, such as the column a task moved to. */
    detail: v.optional(v.string()),
  })
    .index("by_show", ["showId"])
    .index("by_task", ["taskId"]),
  checklistItems: defineTable({
    title: v.string(),
    done: v.boolean(),
    ownerAccount: v.id("users"),
  }).index("by_owner_title", ["ownerAccount", "title"]),
});
