import { faker } from "@faker-js/faker";
import { CoPlainText } from "jazz-tools";
import { Task, TodoProject } from "./1_schema";

export function generateRandomProject(numTasks: number) {
  // Create a list of tasks
  const tasks = TodoProject.shape.tasks.create([]);
  const start = performance.now();

  // Generate random tasks
  function populateTasks() {
    for (let i = 0; i < numTasks; i++) {
      const task = Task.create({
        done: faker.datatype.boolean(),
        text: CoPlainText.create(
          faker.lorem.sentence({ min: 3, max: 8 }),
          tasks.$jazz.owner,
        ),
      });
      tasks.$jazz.push(task);
    }
  }

  // Create and return the project
  return {
    value: TodoProject.create({
      title: `${numTasks} tasks`,
      tasks: tasks,
    }),
    done: new Promise((resolve) => {
      setTimeout(() => {
        populateTasks();
        tasks.$jazz.localNode.syncManager.waitForAllCoValuesSync().then(() => {
          console.log(
            `Generated and synced ${numTasks} tasks in ${performance.now() - start}ms`,
          );
          resolve(true);
        });
      }, 10);
    }),
  };
}
