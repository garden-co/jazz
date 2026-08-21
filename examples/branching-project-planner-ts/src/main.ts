import { BrowserAuthSecretStore, createDb, type BranchSelector, type BranchView } from "jazz-tools";
import { app, type Task } from "../schema.js";

const appId = "branching-project-planner-example";
const secret = await BrowserAuthSecretStore.getOrCreateSecret({ appId });
const db = await createDb({
  appId,
  driver: { type: "memory" },
  auth: { localFirstSecret: secret },
});

const mainScenario = db.insert(app.scenarios, {
  name: "Main plan",
  status: "open",
}).value;
const draftScenario = db.insert(app.scenarios, {
  name: "Fast-track launch",
  base_scenario_id: mainScenario.id,
  status: "open",
}).value;

const selector = (scenarioId: string): BranchSelector => ({
  values: { scenario_id: { type: "Uuid", value: scenarioId } },
});
const main = selector(mainScenario.id);
const draft = selector(draftScenario.id);
const draftOverMain: BranchView = {
  head: draft,
  base: { kind: "current", branch: main },
};

const inherited = db.insert(
  app.tasks,
  { scenario_id: mainScenario.id, title: "Ship documentation", estimate: 5 },
  { branch: main },
).value;
db.insert(
  app.tasks,
  { scenario_id: mainScenario.id, title: "Run migration rehearsal", estimate: 8 },
  { branch: main },
);
db.update(app.scenarios, mainScenario.id, { status: "approved" });
db.insert(
  app.tasks,
  { scenario_id: draftScenario.id, title: "Book launch livestream", estimate: 3 },
  { branch: draft },
);

function render(target: string, tasks: Task[], editable: boolean) {
  const list = document.querySelector<HTMLUListElement>(target);
  if (!list) return;
  list.replaceChildren(
    ...tasks.map((task) => {
      const item = document.createElement("li");
      item.textContent = `${task.title} — ${task.estimate} points`;
      if (editable && task.id === inherited.id) {
        const button = document.createElement("button");
        button.textContent = "Fast-track in draft";
        button.onclick = () => {
          db.update(app.tasks, task.id, { estimate: 2 }, { branch: draftOverMain });
          button.disabled = true;
        };
        item.append(button);
      }
      return item;
    }),
  );
}

db.subscribeAll(app.tasks.orderBy("title"), ({ all }) => render("#main-tasks", all, false), {
  branch: { head: main },
});
db.subscribeAll(app.tasks.orderBy("title"), ({ all }) => render("#draft-tasks", all, true), {
  branch: draftOverMain,
});
