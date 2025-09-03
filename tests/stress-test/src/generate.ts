import { faker } from "@faker-js/faker";
import { MAX_PRIORITY, MIN_PRIORITY, Task, TodoProject } from "./1_schema";

export function generateRandomProject(numTasks: number) {
  // Create a list of tasks
  const tasks = TodoProject.shape.tasks.create([]);

  // Generate random tasks
  function populateTasks() {
    for (let i = 0; i < numTasks; i++) {
      const task = Task.create({
        done: faker.datatype.boolean(),
        text: faker.lorem.sentence({ min: 3, max: 8 }),
        priority: getRandomInt(MIN_PRIORITY, MAX_PRIORITY),
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
