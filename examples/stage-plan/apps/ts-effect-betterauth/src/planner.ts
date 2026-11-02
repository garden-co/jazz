import { Effect, Fiber, Stream } from "effect";
import type { JazzDb } from "jazz-tools/effect";
import { app, TASK_STATUSES, type Crew, type Show, type Task, type TaskStatus } from "../schema";
import { field, h } from "./dom";
import { addTask, createShow, moveTask, rankAfter, STATUS_LABELS, type Me } from "./model";

/**
 * StagePlan's planner, with plain DOM over Effect: each live query is a
 * Stream from the Jazz service, drawn as it changes; writes are the Effects
 * in model.ts. Disposing interrupts every stream.
 */
export function plannerScreen(jazz: JazzDb, account: string, onSignOut: () => void) {
  const fibers: Fiber.Fiber<unknown, unknown>[] = [];
  /** Run a stream for as long as the planner is shown. */
  const watch = <T>(stream: Stream.Stream<T, unknown>, draw: (value: T) => void) => {
    const fiber = Effect.runFork(
      Stream.runForEach(stream, (value) => Effect.sync(() => draw(value))),
    );
    fibers.push(fiber);
    return () => void Effect.runFork(Fiber.interrupt(fiber));
  };
  const run = (effect: Effect.Effect<unknown, unknown>) =>
    Effect.runPromise(effect).catch((cause) => showError(cause));

  const error = h("p", { class: "error", role: "alert", hidden: true });
  const showError = (cause: unknown) => {
    error.textContent = cause instanceof Error ? cause.message : String(cause);
    error.hidden = false;
  };
  const body = h(
    "div",
    { class: "shell-body" },
    h("p", { class: "status", role: "status" }, "Loading your crew profile…"),
  );
  const element = h(
    "main",
    { class: "shell" },
    h(
      "header",
      { class: "top" },
      h("h1", {}, "StagePlan"),
      h("button", { type: "button", class: "ghost", on: { click: onSignOut } }, "Sign out"),
    ),
    error,
    body,
  );

  let me: Me | undefined;
  let shows: Show[] = [];
  let selectedId: string | undefined;
  let stopBoard: (() => void) | undefined;
  let boardFor: string | undefined;

  const showsList = h("ul", { class: "shows" });
  const showsCard = h(
    "section",
    { class: "card", "aria-label": "Shows" },
    h("h2", {}, "Shows"),
    showsList,
    showForm(),
  );
  const boardSlot = h("div");

  watch(jazz.stream(app.crew.where({ account })), (profiles: Crew[]) => {
    const profile = profiles[0];
    if (!profile) {
      me = undefined;
      body.replaceChildren(profileForm());
      return;
    }
    const first = !me;
    me = { account, profile };
    if (first) body.replaceChildren(showsCard, boardSlot);
    drawShows();
  });
  watch(jazz.stream(app.shows), (rows: Show[]) => {
    shows = rows;
    if (me) drawShows();
  });

  function profileForm() {
    const name = h("input", { required: true, autocomplete: "name" });
    return h(
      "form",
      {
        class: "card",
        on: {
          submit: (event) => {
            event.preventDefault();
            if (name.value.trim())
              void run(jazz.insert(app.crew, { account, name: name.value.trim() }));
          },
        },
      },
      h("h2", {}, "Set up your crew profile"),
      field("Your name", name),
      h("button", { type: "submit", class: "primary" }, "Continue"),
    );
  }

  function showForm() {
    const name = h("input", { required: true, placeholder: "Album launch" });
    const venue = h("input", { required: true, placeholder: "The Lantern" });
    const date = h("input", { type: "date", required: true });
    const doors = h("input", { type: "time", required: true, value: "19:00" });
    return h(
      "form",
      {
        on: {
          submit: (event) => {
            event.preventDefault();
            if (!me || !name.value.trim()) return;
            const input = {
              name: name.value.trim(),
              venue: venue.value,
              date: date.value,
              doors: doors.value,
            };
            void run(
              createShow(jazz, me, input).pipe(
                Effect.tap((show) =>
                  Effect.sync(() => {
                    selectedId = show.id;
                    drawShows();
                  }),
                ),
              ),
            );
            name.value = venue.value = date.value = "";
            doors.value = "19:00";
          },
        },
      },
      field("Show", name),
      field("Venue", venue),
      field("Date", date),
      field("Doors", doors),
      h("button", { type: "submit", class: "primary" }, "Add show"),
    );
  }

  function drawShows() {
    const selected = shows.find((show) => show.id === selectedId) ?? shows[0];
    showsList.replaceChildren(
      ...(shows.length
        ? shows.map((show) =>
            h(
              "li",
              {},
              h(
                "button",
                {
                  type: "button",
                  "aria-current": show.id === selected?.id ? "true" : undefined,
                  on: {
                    click: () => {
                      selectedId = show.id;
                      drawShows();
                    },
                  },
                },
                show.name,
              ),
            ),
          )
        : [h("li", { class: "secondary" }, "No shows yet. Add your first one.")]),
    );
    if (!selected || !me) {
      stopBoard?.();
      stopBoard = boardFor = undefined;
      boardSlot.replaceChildren();
      return;
    }
    if (boardFor === selected.id) return;
    stopBoard?.();
    boardFor = selected.id;
    const board = boardView(selected, me);
    stopBoard = board.dispose;
    boardSlot.replaceChildren(board.element);
  }

  function boardView(show: Show, me: Me) {
    let tasks: Task[] = [];
    const column = (status: TaskStatus) =>
      tasks.filter((task) => task.status === status).sort((a, b) => a.rank - b.rank);
    const columns = new Map(TASK_STATUSES.map((status) => [status, h("ul")] as const));
    const title = h("input", { placeholder: "Line check" });
    const checklist = h("button", { type: "button" }, "Add standard checklist");
    checklist.addEventListener("click", () => {
      checklist.disabled = true;
      // Runs on the server, as this user: see server/checklist.ts.
      void fetch(`/api/shows/${show.id}/checklist`, { method: "POST", credentials: "same-origin" })
        .then(async (response) => {
          if (!response.ok)
            throw new Error(
              ((await response.json()) as { error?: string }).error ??
                "Could not add the checklist",
            );
        })
        .catch(showError)
        .finally(() => (checklist.disabled = false));
    });
    const element = h(
      "section",
      { class: "card", "aria-label": `Board for ${show.name}` },
      h(
        "div",
        { class: "top" },
        h(
          "div",
          {},
          h("h2", {}, show.name),
          h("p", { class: "secondary" }, `${show.venue} · ${show.date} · doors ${show.doors}`),
        ),
        h(
          "form",
          {
            on: {
              submit: (event) => {
                event.preventDefault();
                const text = title.value.trim();
                if (!text) return;
                title.value = "";
                void run(addTask(jazz, me, show.id, text, rankAfter(column("todo"))));
              },
            },
          },
          field("New task", title),
          h("button", { type: "submit", class: "primary" }, "Add task"),
          checklist,
        ),
      ),
      h(
        "div",
        { class: "board" },
        ...TASK_STATUSES.map((status) =>
          h(
            "section",
            { class: "column", "aria-label": STATUS_LABELS[status] },
            h("h3", {}, STATUS_LABELS[status]),
            columns.get(status)!,
          ),
        ),
      ),
    );
    const draw = () => {
      for (const status of TASK_STATUSES)
        columns.get(status)!.replaceChildren(
          ...column(status).map((task) =>
            h(
              "li",
              { class: "task" },
              h("span", {}, task.title),
              h(
                "select",
                {
                  "aria-label": `Status of ${task.title}`,
                  on: {
                    change: (event) => {
                      const next = (event.currentTarget as HTMLSelectElement).value as TaskStatus;
                      void run(moveTask(jazz, me, task, next, rankAfter(column(next))));
                    },
                  },
                },
                ...TASK_STATUSES.map((option) =>
                  h(
                    "option",
                    { value: option, selected: option === task.status },
                    STATUS_LABELS[option],
                  ),
                ),
              ),
            ),
          ),
        );
    };
    const stop = watch(jazz.stream(app.tasks.where({ showId: show.id })), (rows: Task[]) => {
      tasks = rows;
      draw();
    });
    return { element, dispose: stop };
  }

  return {
    element,
    dispose() {
      for (const fiber of fibers) Effect.runFork(Fiber.interrupt(fiber));
    },
  };
}
