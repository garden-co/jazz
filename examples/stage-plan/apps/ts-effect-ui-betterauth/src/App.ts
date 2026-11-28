import { UiSchema } from "@effect/ui";
import { Html } from "@effect/ui/html";
import { Schema, Stream } from "effect";
import { Subscription } from "@effect/ui";
import { JazzSessionState, sessionState } from "jazz-tools/effect-ui";
import { Auth, AuthState } from "./auth";
import { Planner } from "./Planner";
import { SignIn } from "./SignIn";

export const AppMessage = UiSchema.TaggedEnum({
  ChangedAuth: { auth: AuthState },
  ChangedSession: { session: JazzSessionState },
});

/**
 * StagePlan's root: it follows Better Auth's session and Jazz's, and shows
 * the sign-in form, the planner for the signed-in account, or a status.
 */
export const App = Html.component({
  name: "App",
  model: Schema.Struct({ auth: AuthState, session: JazzSessionState }),
  message: AppMessage,
}).make({
  init: () => ({ auth: "pending" as const, session: { _tag: "Starting" as const } }),
  subscriptions: () => [
    Subscription.make(
      "auth",
      Stream.unwrap(Auth.useSync(({ state }) => state)).pipe(
        Stream.map((auth) => AppMessage.ChangedAuth({ auth })),
      ),
    ),
    sessionState((session) => AppMessage.ChangedSession({ session })),
  ],
  update: {
    ChangedAuth: ($, { auth }) => $.evolve({ auth }),
    ChangedSession: ($, { session }) => $.evolve({ session }),
  },
  view: (h, { auth, session }) => {
    const status = (message: string, error = false) =>
      h.main.class("shell").children(
        h.p
          .class(error ? "status error" : "status")
          .attr("role", error ? "alert" : "status")
          .children(message),
      );
    if (auth === "pending") return status("Opening StagePlan…");
    if (auth === "signed-out") return SignIn(h, {});
    switch (session._tag) {
      case "Failed":
        return status(session.error, true);
      case "Ready":
        return Planner(h, { props: { account: session.accountId } });
      default:
        return status("Opening StagePlan…");
    }
  },
});
