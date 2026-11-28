import { Command, UiSchema } from "@effect/ui";
import { Html } from "@effect/ui/html";
import { Effect, Schema } from "effect";
import { Jazz, JazzError, JazzWriteRejected } from "jazz-tools/effect";
import { liveQuery } from "jazz-tools/effect-ui";
import { app } from "../jazz.generated";
import { TASK_STATUSES, type TaskStatus } from "../app-extras";
import { addTask, moveTask, rankAfter, STATUS_LABELS } from "./model";
import { Crew, Show, Task, TaskStatus as TaskStatusSchema } from "../jazz.effect.generated";
import { field } from "./view";

export const Me = Schema.Struct({ account: Schema.String, profile: Crew });

export const BoardMessage = UiSchema.TaggedEnum({
  GotTasks: { tasks: Schema.Array(Task) },
  FailedTasks: { error: JazzError },
  ChangedTitle: { value: Schema.String },
  SubmittedTask: {},
  ChangedStatus: { task: Task, status: TaskStatusSchema },
  ClickedChecklist: {},
  CompletedWrite: {},
  FailedWrite: { error: Schema.Union([JazzError, JazzWriteRejected]) },
  CompletedChecklist: {},
  FailedChecklist: { message: Schema.String },
});

const wrote = Effect.match({
  onSuccess: () => BoardMessage.CompletedWrite(),
  onFailure: (error: JazzError | JazzWriteRejected) => BoardMessage.FailedWrite({ error }),
});

/** Add a task at the end of "To do", and log it, in one transaction. */
export class AddTask extends Command.Service<AddTask>()("stage-plan/AddTask", {
  args: { me: Me, showId: Schema.String, title: Schema.String, rank: Schema.Number },
  success: BoardMessage,
  mode: "queue",
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const jazz = yield* Jazz;
      return ({ me, showId, title, rank }) => wrote(addTask(jazz, me, showId, title, rank));
    }),
  );
}

/** Move a task to the end of another column, and log the move. */
export class MoveTask extends Command.Service<MoveTask>()("stage-plan/MoveTask", {
  args: { me: Me, task: Task, status: TaskStatusSchema, rank: Schema.Number },
  success: BoardMessage,
  mode: "queue",
}) {
  static readonly layer = this.toLayer(
    Effect.gen(function* () {
      const jazz = yield* Jazz;
      return ({ me, task, status, rank }) => wrote(moveTask(jazz, me, task, status, rank));
    }),
  );
}

/**
 * Ask the server to add the standard checklist: it runs the same Effect as
 * this user (server/checklist.ts), and the rows arrive through `GotTasks`.
 */
export class AddChecklist extends Command.Service<AddChecklist>()("stage-plan/AddChecklist", {
  args: { showId: Schema.String },
  success: BoardMessage,
}) {
  static readonly layer = this.toLayer(({ showId }) =>
    Effect.tryPromise(async () => {
      const response = await fetch(`/api/shows/${showId}/checklist`, {
        method: "POST",
        credentials: "same-origin",
      });
      if (!response.ok)
        throw new Error(
          ((await response.json()) as { error?: string }).error ?? "Could not add the checklist",
        );
    }).pipe(
      Effect.match({
        onSuccess: () => BoardMessage.CompletedChecklist(),
        onFailure: (error) =>
          BoardMessage.FailedChecklist({
            message: error.cause instanceof Error ? error.cause.message : String(error.cause),
          }),
      }),
    ),
  );
}

const column = (tasks: ReadonlyArray<typeof Task.Type>, status: TaskStatus) =>
  tasks.filter((task) => task.status === status).toSorted((a, b) => a.rank - b.rank);

export const Board = Html.component({
  name: "Board",
  props: Schema.Struct({ show: Show, me: Me }),
  model: Schema.Struct({
    tasks: Schema.Array(Task),
    title: Schema.String,
    addingChecklist: Schema.Boolean,
    error: Schema.optionalKey(Schema.String),
  }),
  message: BoardMessage,
}).make({
  init: () => ({ tasks: [], title: "", addingChecklist: false }),
  subscriptions: ({ props }) => [
    liveQuery("tasks", props.show.id, {
      query: (showId) => app.tasks.where({ showId }),
      onRows: (tasks) => BoardMessage.GotTasks({ tasks }),
      onError: (error) => BoardMessage.FailedTasks({ error }),
    }),
  ],
  update: {
    GotTasks: ($, { tasks }) => $.evolve({ tasks }),
    FailedTasks: ($, { error }) => $.set({ ...$.model, error: error.message }),
    ChangedTitle: ($, { value }) => $.evolve({ title: value }),
    SubmittedTask: ($) => {
      const title = $.model.title.trim();
      if (!title) return $.model;
      return $.evolve({ title: "" }).run(
        AddTask.make({
          me: $.props.me,
          showId: $.props.show.id,
          title,
          rank: rankAfter(column($.model.tasks, "todo")),
        }),
      );
    },
    ChangedStatus: ($, { task, status }) =>
      $.run(
        MoveTask.make({
          me: $.props.me,
          task,
          status,
          rank: rankAfter(column($.model.tasks, status)),
        }),
      ),
    ClickedChecklist: ($) =>
      $.evolve({ addingChecklist: true }).run(AddChecklist.make({ showId: $.props.show.id })),
    CompletedWrite: ($) => $.model,
    FailedWrite: ($, { error }) => $.set({ ...$.model, error: error.message }),
    CompletedChecklist: ($) => $.evolve({ addingChecklist: false }),
    FailedChecklist: ($, { message }) =>
      $.set({ ...$.model, addingChecklist: false, error: message }),
  },
  view: (h, model) => {
    const { show } = h.props;
    return h.section
      .class("card")
      .attr("aria-label", `Board for ${show.name}`)
      .children(
        h.div.class("top").children(
          h.div.children(
            h.h2.children(show.name),
            h.p.class("secondary").children(`${show.venue} · ${show.date} · doors ${show.doors}`),
          ),
          h.form.on("submit", BoardMessage.SubmittedTask()).children(
            field(
              h,
              "New task",
              h.input
                .attr("placeholder", "Line check")
                .value(model.title)
                .on("input", ({ value }) => BoardMessage.ChangedTitle({ value })),
            ),
            h.button.attr("type", "submit").class("primary").children("Add task"),
            h.button
              .attr("type", "button")
              .disabled(model.addingChecklist)
              .on("click", BoardMessage.ClickedChecklist())
              .children("Add standard checklist"),
          ),
        ),
        model.error !== undefined && h.p.class("error").attr("role", "alert").children(model.error),
        h.div.class("board").children(
          TASK_STATUSES.map((status) =>
            h.section
              .key(status)
              .class("column")
              .attr("aria-label", STATUS_LABELS[status])
              .children(
                h.h3.children(STATUS_LABELS[status]),
                h.ul.children(
                  column(model.tasks, status).map((task) =>
                    h.li
                      .key(task.id)
                      .class("task")
                      .children(
                        h.span.children(task.title),
                        h.select
                          .attr("aria-label", `Status of ${task.title}`)
                          .value(task.status)
                          .on("change", ({ value }) =>
                            BoardMessage.ChangedStatus({
                              task,
                              status: Schema.decodeUnknownSync(TaskStatusSchema)(value),
                            }),
                          )
                          .children(
                            TASK_STATUSES.map((option) =>
                              h.option
                                .key(option)
                                .attr("value", option)
                                .children(STATUS_LABELS[option]),
                            ),
                          ),
                      ),
                  ),
                ),
              ),
          ),
        ),
      );
  },
});
