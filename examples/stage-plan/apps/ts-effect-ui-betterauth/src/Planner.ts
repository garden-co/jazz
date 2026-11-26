import { Command, Component, UiSchema } from "@effect/ui";
import { Html } from "@effect/ui/html";
import { Effect, Layer, Option, Schema } from "effect";
import { Jazz, JazzError, JazzWriteRejected } from "jazz-tools/effect";
import { JazzSession, liveQuery } from "jazz-tools/effect-ui";
import { app } from "../jazz.generated";
import { Auth } from "./auth";
import { AddChecklist, AddTask, Board, Me, MoveTask } from "./Board";
import { createShow } from "./model";
import { Crew, Show } from "./rows";
import { field } from "./view";

const ShowForm = Schema.Struct({
  name: Schema.String,
  venue: Schema.String,
  date: Schema.String,
  doors: Schema.String,
});
type ShowForm = typeof ShowForm.Type;
const emptyShowForm: ShowForm = { name: "", venue: "", date: "", doors: "19:00" };

export const PlannerMessage = UiSchema.TaggedEnum({
  GotProfiles: { profiles: Schema.Array(Crew) },
  GotShows: { shows: Schema.Array(Show) },
  FailedQuery: { error: JazzError },
  ChangedProfileName: { value: Schema.String },
  SubmittedProfile: {},
  ChangedShowForm: { field: Schema.Literals(["name", "venue", "date", "doors"]), value: Schema.String },
  SubmittedShow: {},
  CreatedShow: { show: Show },
  CompletedWrite: {},
  FailedWrite: { error: Schema.Union([JazzError, JazzWriteRejected]) },
  SelectedShow: { id: Schema.String },
  ClickedSignOut: {},
  FailedSignOut: { message: Schema.String },
  FailedJazz: { message: Schema.String },
});

export class CreateProfile extends Command.Service<CreateProfile>()("stage-plan/CreateProfile", {
  args: { account: Schema.String, name: Schema.String },
  success: PlannerMessage,
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const jazz = yield* Jazz;
      return ({ account, name }) =>
        jazz.insert(app.crew, { account, name }).pipe(
          Effect.match({
            onSuccess: () => PlannerMessage.CompletedWrite(),
            onFailure: (error) => PlannerMessage.FailedWrite({ error }),
          }),
        );
    }),
  );
}

/** Create a show with its creator as crew chief and a first invite, in one transaction. */
export class CreateShow extends Command.Service<CreateShow>()("stage-plan/CreateShow", {
  args: { me: Me, input: ShowForm },
  success: PlannerMessage,
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const jazz = yield* Jazz;
      return ({ me, input }) =>
        createShow(jazz, me, input).pipe(
          Effect.match({
            onSuccess: (show) => PlannerMessage.CreatedShow({ show }),
            onFailure: (error) => PlannerMessage.FailedWrite({ error }),
          }),
        );
    }),
  );
}

/** Sign out of Better Auth, then Jazz follows the backend's answer. */
export class SignOut extends Command.Service<SignOut>()("stage-plan/SignOut", {
  args: {},
  success: Schema.Union([PlannerMessage, Component.Noop]),
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const auth = yield* Auth;
      const session = yield* JazzSession;
      return () =>
        auth.signOut.pipe(
          Effect.andThen(session.refresh),
          Effect.match({
            onSuccess: () => Component.noop,
            onFailure: (error) => PlannerMessage.FailedSignOut({ message: error.message }),
          }),
        );
    }),
  );
}

const PlannerView = Html.component({
  name: "Planner",
  props: Schema.Struct({ account: Schema.String }),
  model: Schema.Struct({
    /** `None` until the profile query first answers. */
    profiles: Schema.Option(Schema.Array(Crew)),
    shows: Schema.Array(Show),
    selectedId: Schema.optionalKey(Schema.String),
    profileName: Schema.String,
    showForm: ShowForm,
    error: Schema.optionalKey(Schema.String),
  }),
  message: PlannerMessage,
}).make({
  init: () => ({
    profiles: Option.none(),
    shows: [],
    profileName: "",
    showForm: emptyShowForm,
  }),
  subscriptions: ({ props }) => [
    liveQuery("profile", props.account, {
      query: (account) => app.crew.where({ account }),
      onRows: (profiles) => PlannerMessage.GotProfiles({ profiles }),
      onError: (error) => PlannerMessage.FailedQuery({ error }),
    }),
    liveQuery("shows", null, {
      query: () => app.shows,
      onRows: (shows) => PlannerMessage.GotShows({ shows }),
      onError: (error) => PlannerMessage.FailedQuery({ error }),
    }),
  ],
  update: {
    GotProfiles: ($, { profiles }) => $.evolve({ profiles: Option.some(profiles) }),
    GotShows: ($, { shows }) => $.evolve({ shows }),
    FailedQuery: ($, { error }) => $.set({ ...$.model, error: error.message }),
    ChangedProfileName: ($, { value }) => $.evolve({ profileName: value }),
    SubmittedProfile: ($) => {
      const name = $.model.profileName.trim();
      return name ? $.run(CreateProfile.make({ account: $.props.account, name })) : $.model;
    },
    ChangedShowForm: ($, { field, value }) =>
      $.evolve({ showForm: (form) => ({ ...form, [field]: value }) }),
    SubmittedShow: ($) => {
      const profile = Option.flatMap($.model.profiles, (profiles) => Option.fromNullishOr(profiles[0]));
      const input = { ...$.model.showForm, name: $.model.showForm.name.trim() };
      if (Option.isNone(profile) || !input.name) return $.model;
      return $.evolve({ showForm: emptyShowForm }).run(
        CreateShow.make({ me: { account: $.props.account, profile: profile.value }, input }),
      );
    },
    CreatedShow: ($, { show }) => $.set({ ...$.model, selectedId: show.id }),
    CompletedWrite: ($) => $.model,
    FailedWrite: ($, { error }) => $.set({ ...$.model, error: error.message }),
    SelectedShow: ($, { id }) => $.set({ ...$.model, selectedId: id }),
    ClickedSignOut: ($) => $.run(SignOut.make({})),
    FailedSignOut: ($, { message }) => $.set({ ...$.model, error: message }),
    FailedJazz: ($, { message }) => $.set({ ...$.model, error: message }),
  },
  view: (h, model) => {
    const profile = Option.flatMap(model.profiles, (profiles) => Option.fromNullishOr(profiles[0]));
    const selected = model.shows.find((show) => show.id === model.selectedId) ?? model.shows[0];
    const showField = (label: string, key: keyof ShowForm, type: string, placeholder?: string) =>
      field(
        h,
        label,
        h.input
          .attr("type", type)
          .attr("placeholder", placeholder ?? "")
          .value(model.showForm[key])
          .on("input", ({ value }) => PlannerMessage.ChangedShowForm({ field: key, value })),
      );
    const body = Option.match(model.profiles, {
      onNone: () => h.p.class("status").attr("role", "status").children("Loading your crew profile…"),
      onSome: () =>
        Option.match(profile, {
          onNone: () =>
            h.form
              .class("card")
              .on("submit", PlannerMessage.SubmittedProfile())
              .children(
                h.h2.children("Set up your crew profile"),
                field(
                  h,
                  "Your name",
                  h.input
                    .attr("autocomplete", "name")
                    .value(model.profileName)
                    .on("input", ({ value }) => PlannerMessage.ChangedProfileName({ value })),
                ),
                h.button.attr("type", "submit").class("primary").children("Continue"),
              ),
          onSome: (profile) =>
            h.div.children(
              h.section
                .class("card")
                .attr("aria-label", "Shows")
                .children(
                  h.h2.children("Shows"),
                  h.ul.class("shows").children(
                    model.shows.length
                      ? model.shows.map((show) =>
                          h.li.key(show.id).children(
                            h.button
                              .attr("type", "button")
                              .attr("aria-current", show.id === selected?.id ? "true" : undefined)
                              .on("click", PlannerMessage.SelectedShow({ id: show.id }))
                              .children(show.name),
                          ),
                        )
                      : [h.li.class("secondary").children("No shows yet. Add your first one.")],
                  ),
                  h.form.on("submit", PlannerMessage.SubmittedShow()).children(
                    showField("Show", "name", "text", "Album launch"),
                    showField("Venue", "venue", "text", "The Lantern"),
                    showField("Date", "date", "date"),
                    showField("Doors", "doors", "time"),
                    h.button.attr("type", "submit").class("primary").children("Add show"),
                  ),
                ),
              // Keyed on the show: switching shows mounts a fresh board and
              // closes the old one's live query.
              selected &&
                Board(h, {
                  key: selected.id,
                  props: { show: selected, me: { account: h.props.account, profile } },
                }),
            ),
        }),
    });
    return h.main.class("shell").children(
      h.header.class("top").children(
        h.h1.children("StagePlan"),
        h.button
          .attr("type", "button")
          .class("ghost")
          .on("click", PlannerMessage.ClickedSignOut())
          .children("Sign out"),
      ),
      model.error !== undefined && h.p.class("error").attr("role", "alert").children(model.error),
      h.div.class("shell-body").children(body),
    );
  },
});

/**
 * The planner, with `Jazz` for the signed-in account. Keyed on the account:
 * the session replaces its Db when the user changes, and a new key builds
 * the layer again against the new one.
 */
export const Planner = Component.provide(
  PlannerView,
  {
    key: (props) => props.account,
    layer: () =>
      Layer.mergeAll(
        CreateProfile.layer,
        CreateShow.layer,
        AddTask.layer,
        MoveTask.layer,
        AddChecklist.layer,
        SignOut.layer,
      ).pipe(Layer.provideMerge(JazzSession.jazz)),
  },
  {
    onFailure: () => PlannerMessage.FailedJazz({ message: "Jazz has no signed-in user" }),
    server: false,
  },
);
