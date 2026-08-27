import { BrowserAuthSecretStore, createDb } from "jazz-tools";
import { app, type Scenario, type Task } from "../schema.js";

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

function readScenario(scenario: Scenario) {
  return scenario.base_scenario_id
    ? ({ branch: scenario.id, base: scenario.base_scenario_id } as const)
    : ({ branch: scenario.id } as const);
}

const mainView = readScenario(mainScenario);
const draftView = readScenario(draftScenario);

const inherited = db.insert(
  app.tasks,
  { scenario_id: mainScenario.id, title: "Ship documentation", estimate: 5 },
  { branch: mainScenario.id },
).value;
db.insert(
  app.tasks,
  { scenario_id: mainScenario.id, title: "Run migration rehearsal", estimate: 8 },
  { branch: mainScenario.id },
);
db.update(app.scenarios, mainScenario.id, { status: "approved" });
db.insert(
  app.tasks,
  { scenario_id: draftScenario.id, title: "Book launch livestream", estimate: 3 },
  { branch: draftScenario.id },
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
          db.update(app.tasks, task.id, { estimate: 2 }, draftView);
          button.disabled = true;
        };
        item.append(button);
      }
      return item;
    }),
  );
}

db.subscribe(app.tasks.orderBy("title"), (tasks) => render("#main-tasks", tasks, false), mainView);
db.subscribe(app.tasks.orderBy("title"), (tasks) => render("#draft-tasks", tasks, true), draftView);
