import { faker } from "@faker-js/faker";
import { CoPlainText } from "jazz-tools";
import { Task, TodoProject } from "./1_schema";

export function generateRandomProject(numTasks: number) {
  // Generate a random project title
  const projectTitle = faker.company.catchPhrase();

  // Create a list of tasks
  const tasks = TodoProject.def.shape.tasks.create([]);

  // Generate random tasks
  function populateTasks() {
    for (let i = 0; i < numTasks; i++) {
      const task = Task.create({
        done: faker.datatype.boolean(),
        text: CoPlainText.create(
          faker.lorem.sentence({ min: 3, max: 8 }),
          tasks._owner,
        ),
        version: 1,
      });
      tasks.push(task);
    }
  }

  // Create and return the project
  return {
    value: TodoProject.create({
      title: projectTitle,
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
