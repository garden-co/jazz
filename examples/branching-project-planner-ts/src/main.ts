import { createAccountManager, createDb, parseSnapshotRef, serializeSnapshotRef } from "jazz-tools";
import { app, type Task } from "../schema.js";

const appId = "branching-project-planner-example";
// The account registry names its authority; this database runs without a server.
const accounts = await createAccountManager({ appId, serverUrl: location.origin });
const account = accounts.getLoggedIn() ?? accounts.createLocalFirst();
const db = await createDb({ appId, driver: { type: "persistent" }, account });

let mainScenario = await db.one(app.scenarios.where({ name: "Main plan" }));
if (!mainScenario) {
  mainScenario = db.insert(app.scenarios, { name: "Main plan", status: "open" }).value;
  db.insert(
    app.tasks,
    { scenario_id: mainScenario.id, title: "Ship documentation", estimate: 5 },
    { branch: mainScenario.id },
  );
  db.insert(
    app.tasks,
    { scenario_id: mainScenario.id, title: "Run migration rehearsal", estimate: 8 },
    { branch: mainScenario.id },
  );
}

let draftScenario = await db.one(app.scenarios.where({ name: "Fast-track launch" }));
if (!draftScenario) {
  const captured = await db.captureSnapshot();
  draftScenario = db.insert(app.scenarios, {
    name: "Fast-track launch",
    base_scenario_id: mainScenario.id,
    base_snapshot_ref: serializeSnapshotRef(captured),
    status: "open",
  }).value;
  await db
    .insert(
      app.tasks,
      { scenario_id: draftScenario.id, title: "Book launch livestream", estimate: 3 },
      { branch: draftScenario.id },
    )
    .wait({ tier: "local" });
}

if (!draftScenario.base_scenario_id || !draftScenario.base_snapshot_ref)
  throw new Error("Draft has no frozen base");
// On reopening, restore the saved reference against the retained dataset.
const restored = parseSnapshotRef(draftScenario.base_snapshot_ref);
const mainView = { branch: mainScenario.id } as const;
const liveDraftView = { branch: draftScenario.id, base: draftScenario.base_scenario_id } as const;
const frozenDraftView = {
  branch: draftScenario.id,
  base: [draftScenario.base_scenario_id, restored],
} as const;
const inherited = await db.one(app.tasks, mainView);

function report(error: unknown) {
  document.querySelector("#status")!.textContent =
    error instanceof Error ? error.message : String(error);
}

function render(target: string, tasks: Task[], editable: boolean) {
  const list = document.querySelector<HTMLUListElement>(target)!;
  list.replaceChildren(
    ...tasks.map((task) => {
      const item = document.createElement("li");
      item.textContent = `${task.title} — ${task.estimate} points`;
      if (editable && task.id === inherited?.id) {
        const button = document.createElement("button");
        button.textContent = "Fast-track in draft";
        button.onclick = () => {
          void db
            .update(app.tasks, task.id, { estimate: 2 }, frozenDraftView)
            .wait({ tier: "local" })
            .catch(report);
        };
        item.append(button);
      }
      return item;
    }),
  );
}

db.subscribe(
  app.tasks.orderBy("title"),
  { onUpdate: (tasks) => render("#main-tasks", tasks, false), onError: report },
  mainView,
);
db.subscribe(
  app.tasks.orderBy("title"),
  { onUpdate: (tasks) => render("#live-draft-tasks", tasks, false), onError: report },
  liveDraftView,
);
db.subscribe(
  app.tasks.orderBy("title"),
  { onUpdate: (tasks) => render("#draft-tasks", tasks, true), onError: report },
  frozenDraftView,
);

document.querySelector<HTMLButtonElement>("#change-main")!.onclick = () => {
  if (inherited)
    void db
      .update(app.tasks, inherited.id, { title: "Changed published task", estimate: 13 }, mainView)
      .wait({ tier: "local" })
      .catch(report);
};
document.querySelector<HTMLButtonElement>("#reopen")!.onclick = () => {
  void db.shutdown().then(() => location.reload(), report);
};
