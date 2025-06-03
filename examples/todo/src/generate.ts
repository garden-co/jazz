import { faker } from "@faker-js/faker";
import { CoPlainText, Loaded, co } from "jazz-tools";
import { Task, TodoProject } from "./1_schema";

export function generateRandomProject(
  numTasks: number,
): Loaded<typeof TodoProject> {
  // Generate a random project title
  const projectTitle = faker.company.catchPhrase();

  // Create a list of tasks
  const tasks = co.list(Task).create([]);

  // Generate random tasks
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

  // Create and return the project
  return TodoProject.create({
    title: projectTitle,
    tasks: tasks,
  });
}
