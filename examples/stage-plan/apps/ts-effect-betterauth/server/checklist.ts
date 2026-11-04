import { Data, Effect, Option } from "effect";
import { Jazz } from "jazz-tools/effect";
import { app } from "../schema";
import { addChecklist } from "../src/model";
import { jazz } from "./jazz";

class RouteError extends Data.TaggedError("RouteError")<{
  status: 401 | 404 | 409;
  message: string;
}> {}

/**
 * POST /api/shows/:id/checklist: add the standard pre-show checklist. The
 * route acts as the requesting user (`forRequest`), so permissions.ts decides
 * whether they may add tasks to this show, exactly as for the browser.
 */
export function checklistRoute(request: Request, showId: string): Promise<Response> {
  const program = Effect.gen(function* () {
    const backend = yield* Effect.promise(() => jazz());
    const account = yield* Effect.promise(() => backend.accountFor(request));
    if (!account) return yield* new RouteError({ status: 401, message: "sign in required" });
    const db = Jazz.fromDb(yield* Effect.promise(() => backend.forRequest(request)));

    const profile = yield* db.one(app.crew.where({ account }), { tier: "remote" });
    if (Option.isNone(profile))
      return yield* new RouteError({ status: 409, message: "set up your crew profile first" });
    const show = yield* db.one(app.shows.where({ id: showId }), { tier: "remote" });
    if (Option.isNone(show))
      return yield* new RouteError({ status: 404, message: "show not found" });
    const existing = yield* db.all(app.tasks.where({ showId }), { tier: "remote" });

    const added = yield* addChecklist(db, { account, profile: profile.value }, showId, existing);
    return Response.json({ added: added.map((task) => task.title) });
  });
  return Effect.runPromise(
    program.pipe(
      Effect.catchTag("RouteError", (error) =>
        Effect.succeed(Response.json({ error: error.message }, { status: error.status })),
      ),
      Effect.catchTag("JazzWriteRejected", (error) =>
        Effect.succeed(Response.json({ error: error.message }, { status: 403 })),
      ),
    ),
  );
}
