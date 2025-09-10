import { faker } from "@faker-js/faker";
import { MAX_PRIORITY, MIN_PRIORITY, Task, TodoProject } from "./1_schema";
import { CoPlainText } from "jazz-tools";

export function generateRandomProject(numTasks: number) {
  // Create a list of tasks
  const tasks = TodoProject.shape.tasks.create([]);

  // Generate random tasks
  function populateTasks() {
    const newTasks = Array.from({ length: numTasks }, () =>
      Task.create(
        {
          done: faker.datatype.boolean(),
          text: CoPlainText.create(
            faker.lorem.sentence({ min: 3, max: 8 }),
            tasks.$jazz.owner,
          ),
          priority: getRandomInt(MIN_PRIORITY, MAX_PRIORITY),
        },
        { owner: tasks.$jazz.owner },
      ),
    );
    // Temporary workaround to avoid hitting the transaction size limit.
    // Once https://github.com/garden-co/jazz/issues/2887 is implemented,
    // this will happen automatically.
    const chunkSize = 5000;
    for (let i = 0; i < newTasks.length; i += chunkSize) {
      const chunk = newTasks.slice(i, i + chunkSize);
      tasks.$jazz.push(...chunk);
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
        resolve(true);
      }, 10);
    }),
  };
}

/**
 * Get a random integer between min and max.
 * Both the maximum and the minimum are inclusive
 */
function getRandomInt(min: number, max: number): number {
  const minCeiled = Math.ceil(min);
  const maxFloored = Math.floor(max);
  return Math.floor(Math.random() * (maxFloored - minCeiled + 1) + minCeiled);
}
