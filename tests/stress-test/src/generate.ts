import { faker } from "@faker-js/faker";
import { CoPlainText } from "jazz-tools";
import { Task, TodoProject } from "./1_schema";

export function generateRandomProject(numTasks: number) {
  // Create a list of tasks
  const tasks = TodoProject.shape.tasks.create([]);

  // Generate random tasks
  function populateTasks() {
    for (let i = 0; i < numTasks; i++) {
      const task = Task.create({
        done: faker.datatype.boolean(),
        text: CoPlainText.create(
          // In order to test the compression of the text, we generate a long sentence.
          faker.lorem.sentence({ min: 10, max: 20 }),
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
        resolve(true);
      }, 10);
    }),
  };
}
