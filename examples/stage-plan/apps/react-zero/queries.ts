import { defineQueries, defineQuery, escapeLike, type ExpressionBuilder } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData, type Schema } from "./schema.js";

/**
 * Who can read what in StagePlan (the read half of the README's rules):
 *
 * - Crew profiles (names) are shared.
 * - A show, its tasks, comments, activity and crew list are for its crew
 *   (anyone with a showCrew row) and its chief; everyone else sees nothing.
 * - Invite codes are bearer secrets: only the chief reads them.
 * - Checklist items are private to their owner.
 *
 * zero-cache asks the server for each query's ZQL with the caller's context,
 * so these filters are applied server-side; the client runs the same ZQL
 * against what it has synced.
 */
const me = (ctx: AuthData | undefined) => ctx?.userID ?? "";

// A show is visible to its chief and its crew.
const isCrew =
  (ctx: AuthData | undefined) =>
  ({ cmp, exists, or }: ExpressionBuilder<"shows", Schema>) =>
    or(
      cmp("chiefAccount", me(ctx)),
      exists("crew", (crew) => crew.where("account", me(ctx))),
    );
const crewShows = (ctx: AuthData | undefined) => zql.shows.where(isCrew(ctx));

const showArgs = z.object({ showId: z.string() });

export const queries = defineQueries({
  crew: {
    mine: defineQuery(({ ctx }) => zql.crew.where("account", me(ctx)).one()),
  },
  shows: {
    // Read permissions only return shows you crew on, so this is "my shows".
    mine: defineQuery(({ ctx }) => crewShows(ctx).orderBy("date", "asc").related("tasks")),
    one: defineQuery(showArgs, ({ ctx, args }) => crewShows(ctx).where("id", args.showId).one()),
  },
  showCrew: {
    byShow: defineQuery(showArgs, ({ ctx, args }) =>
      zql.showCrew
        .where("showId", args.showId)
        .whereExists("show", (show) => show.where(isCrew(ctx)))
        .related("crew")
        .orderBy("role", "asc"),
    ),
  },
  showInvites: {
    byShow: defineQuery(showArgs, ({ ctx, args }) =>
      zql.showInvites
        .where("showId", args.showId)
        .whereExists("show", (show) => show.where("chiefAccount", me(ctx))),
    ),
  },
  tasks: {
    byShow: defineQuery(showArgs, ({ ctx, args }) =>
      zql.tasks
        .where("showId", args.showId)
        .whereExists("show", (show) => show.where(isCrew(ctx)))
        .orderBy("rank", "asc"),
    ),
  },
  comments: {
    byTask: defineQuery(z.object({ taskId: z.string() }), ({ ctx, args }) =>
      zql.comments
        .where("taskId", args.taskId)
        .whereExists("task", (task) => task.whereExists("show", (show) => show.where(isCrew(ctx))))
        .related("author")
        .orderBy("createdAt", "asc"),
    ),
  },
  activity: {
    feed: defineQuery(
      z.object({ showId: z.string(), taskId: z.string().optional(), limit: z.number() }),
      ({ ctx, args }) => {
        let q = zql.activity.where("showId", args.showId);
        if (args.taskId) q = q.where("taskId", args.taskId);
        return q
          .whereExists("show", (show) => show.where(isCrew(ctx)))
          .related("actor")
          .orderBy("createdAt", "desc")
          .limit(args.limit);
      },
    ),
  },
  checklist: {
    mine: defineQuery(
      z.object({ search: z.string(), filter: z.enum(["all", "open", "done"]) }),
      ({ ctx, args }) => {
        let q = zql.checklistItems.where("ownerAccount", me(ctx));
        if (args.search) q = q.where("title", "LIKE", `%${escapeLike(args.search)}%`);
        if (args.filter !== "all") q = q.where("done", args.filter === "done");
        return q.orderBy("title", "asc");
      },
    ),
  },
});
