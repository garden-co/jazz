import { faker } from "@faker-js/faker";
import { CoPlainText } from "jazz-tools";
import { ListOfTasks, Task, TodoProject } from "./1_schema";

export function generateRandomProject(numTasks: number): TodoProject {
  // Generate a random project title
  const projectTitle = faker.company.catchPhrase();

  // Create a list of tasks
  const tasks = ListOfTasks.create([]);

  // Generate random tasks
  for (let i = 0; i < numTasks; i++) {
    const task = Task.create({
      done: faker.datatype.boolean(),
      text: CoPlainText.create(
        faker.lorem.sentence({ min: 3, max: 8 }),
        tasks._owner,
      ),
    });
    tasks.push(task);
  }

  // Create and return the project
  return TodoProject.create({
    title: projectTitle,
    tasks: tasks,
  });
}
