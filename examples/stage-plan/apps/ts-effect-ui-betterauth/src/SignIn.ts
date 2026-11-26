import { Command, UiSchema } from "@effect/ui";
import { Html } from "@effect/ui/html";
import { Effect, Redacted, Schema } from "effect";
import { JazzSession } from "jazz-tools/effect-ui";
import { Auth } from "./auth";
import { field } from "./view";

export const SignInMessage = UiSchema.TaggedEnum({
  ToggledMode: {},
  ChangedName: { value: Schema.String },
  ChangedEmail: { value: Schema.String },
  ChangedPassword: { value: Schema.Redacted(Schema.String) },
  Submitted: {},
  SucceededSignIn: {},
  FailedSignIn: { message: Schema.String },
});

/**
 * Sign in (or up) with Better Auth, then have Jazz ask the backend who is
 * signed in now. The root hears the result through the session's state.
 */
export class SubmitSignIn extends Command.Service<SubmitSignIn>()("stage-plan/SubmitSignIn", {
  args: {
    signingUp: Schema.Boolean,
    name: Schema.String,
    email: Schema.String,
    password: Schema.Redacted(Schema.String),
  },
  success: SignInMessage,
  mode: "switch",
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const auth = yield* Auth;
      const session = yield* JazzSession;
      return ({ signingUp, name, email, password }) =>
        (signingUp ? auth.signUp({ name, email, password }) : auth.signIn({ email, password })).pipe(
          Effect.andThen(session.refresh),
          Effect.match({
            onSuccess: () => SignInMessage.SucceededSignIn(),
            onFailure: (error) => SignInMessage.FailedSignIn({ message: error.message }),
          }),
        );
    }),
  );
}

export const SignIn = Html.component({
  name: "SignIn",
  model: Schema.Struct({
    signingUp: Schema.Boolean,
    name: Schema.String,
    email: Schema.String,
    password: Schema.Redacted(Schema.String),
    submitting: Schema.Boolean,
    error: Schema.optionalKey(Schema.String),
  }),
  message: SignInMessage,
  // The form holds a password: keep it out of history and page state.
  restore: false,
}).make({
  init: () => ({
    signingUp: false,
    name: "",
    email: "",
    password: Redacted.make(""),
    submitting: false,
  }),
  update: {
    ToggledMode: ($) => $.evolve({ signingUp: (signingUp) => !signingUp }),
    ChangedName: ($, { value }) => $.evolve({ name: value }),
    ChangedEmail: ($, { value }) => $.evolve({ email: value }),
    ChangedPassword: ($, { value }) => $.evolve({ password: value }),
    Submitted: ($) => {
      const { signingUp, name, email, password, submitting } = $.model;
      if (submitting) return $.model;
      return $.set({ signingUp, name, email, password, submitting: true }).run(
        SubmitSignIn.make({ signingUp, name, email, password }),
      );
    },
    // The session's state takes the app to the planner.
    SucceededSignIn: ($) => $.evolve({ submitting: false }),
    FailedSignIn: ($, { message }) => $.set({ ...$.model, submitting: false, error: message }),
  },
  view: (h, model) =>
    h.main.class("shell").children(
      h.form
        .class("card sign-in")
        .on("submit", SignInMessage.Submitted())
        .children(
          h.h1.children(model.signingUp ? "Create your StagePlan account" : "Sign in to StagePlan"),
          h.p
            .class("secondary")
            .children("Plan your shows with your crew: tasks for every gig, live on every device."),
          model.signingUp &&
            field(
              h,
              "Name",
              h.input
                .attr("name", "name")
                .attr("autocomplete", "name")
                .value(model.name)
                .on("input", ({ value }) => SignInMessage.ChangedName({ value })),
            ),
          field(
            h,
            "Email",
            h.input
              .attr("name", "email")
              .attr("type", "email")
              .attr("autocomplete", "email")
              .value(model.email)
              .on("input", ({ value }) => SignInMessage.ChangedEmail({ value })),
          ),
          field(
            h,
            "Password",
            h.input
              .attr("name", "password")
              .attr("type", "password")
              .attr("autocomplete", model.signingUp ? "new-password" : "current-password")
              .value(Redacted.value(model.password))
              .on("input", ({ value }) =>
                SignInMessage.ChangedPassword({ value: Redacted.make(value) }),
              ),
          ),
          model.error !== undefined && h.p.class("error").attr("role", "alert").children(model.error),
          h.button
            .attr("type", "submit")
            .class("primary")
            .attr("disabled", model.submitting)
            .children(model.signingUp ? "Create account" : "Sign in"),
          h.button
            .attr("type", "button")
            .class("ghost")
            .on("click", SignInMessage.ToggledMode())
            .children(model.signingUp ? "I already have an account" : "Create an account"),
        ),
    ),
});
