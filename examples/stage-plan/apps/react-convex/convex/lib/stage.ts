import type { Doc, Id } from "../_generated/dataModel";
import type { MutationCtx } from "../_generated/server";

type Me = { account: Id<"users">; profile: Doc<"crew"> };
export type ShowInput = Pick<Doc<"shows">, "name" | "venue" | "date" | "doors">;
export type ActivityKind = Doc<"activity">["kind"];

export function newInviteCode() {
  // Full UUID entropy: the link is a reusable bearer capability.
  return crypto.randomUUID();
}

/** Inserts a show with the creator as crew chief and a first invite code. */
export async function insertShow(ctx: MutationCtx, me: Me, input: ShowInput) {
  const showId = await ctx.db.insert("shows", { ...input, chiefAccount: me.account });
  await ctx.db.insert("showCrew", {
    showId,
    crewId: me.profile._id,
    account: me.account,
    role: "chief",
  });
  await ctx.db.insert("showInvites", { showId, code: newInviteCode() });
  return showId;
}

export function logActivity(
  ctx: MutationCtx,
  me: Me,
  task: Pick<Doc<"tasks">, "_id" | "showId">,
  kind: ActivityKind,
  detail?: string,
) {
  return ctx.db.insert("activity", {
    showId: task.showId,
    taskId: task._id,
    actorId: me.profile._id,
    kind,
    detail,
  });
}

export const STATUS_LABELS: Record<Doc<"tasks">["status"], string> = {
  todo: "To do",
  doing: "In progress",
  done: "Done",
  blocked: "Blocked",
};
