import { useCallback } from "react";

import { Task, TodoProject } from "./1_schema";

import {
  Button,
  Checkbox,
  Skeleton,
  SubmittableInput,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./basicComponents";

import { Loaded } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import { useParams } from "react-router";
import uniqolor from "uniqolor";
import { Trash2 } from "lucide-react";
import { InviteButton } from "./components/InviteButton";

/** Walkthrough: Reactively rendering a todo project as a table,
 *               adding and editing tasks
 *
 *  Here in `<TodoTable/>`, we use `useCoState()` for the first time,
 *  in this case to load the CoValue for our `TodoProject` as well as
 *  the `ListOfTasks` referenced in it.
 */

export function ProjectTodoTable() {
  const projectId = useParams<{ projectId: string }>().projectId;

  // `useCoState()` reactively subscribes to updates to a CoValue's
  // content - whether we create edits locally, load persisted data, or receive
  // sync updates from other devices or participants!
  // It also recursively resolves and subsribes to all referenced CoValues.
  const project = useCoState(TodoProject, projectId, {
    resolve: {
      tasks: {
        $each: {
          text: true,
        },
      },
    },
  });

  // `createTask` is similar to `createProject` we saw earlier, creating a new CoMap
  // for a new task (in the same group as the project), and then
  // adding that as an item to the project's list of tasks.
  const createTask = useCallback(
    (text: string) => {
      if (!project?.tasks || !text) return;
      const task = Task.create(
        {
          done: false,
          text,
          version: 1,
        },
        project.$jazz.owner,
      );

      // push will cause useCoState to rerender this component, both here and on other devices
      project.tasks.$jazz.push(task);
    },
    [project?.tasks, project?.$jazz.owner],
  );

  const deleteTask = useCallback(
    (taskId: string) => {
      if (!project?.tasks) return;
      // similarly, removing a task will update everyone that is subscribed to this project's tasks
      project.tasks.$jazz.remove((t) => t?.$jazz.id === taskId);
    },
    [project?.tasks],
  );

  return (
    <div className="max-w-full w-xl">
      <div className="flex justify-between items-center gap-4 mb-4">
        <h1>
          {
            // This is how we can access properties from the project query,
            // accounting for the fact that note everything might be loaded yet
            project?.title ? (
              <>
                {project.title}{" "}
                <span className="text-sm">({project.$jazz.id})</span>
              </>
            ) : (
              <Skeleton className="mt-1 w-[200px] h-[1em] rounded-full" />
            )
          }
        </h1>
        <InviteButton value={project} valueHint="project" />
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[40px]">Done</TableHead>
            <TableHead>Task</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {project?.tasks.map(
            (task) =>
              task && (
                <TaskRow
                  key={task.$jazz.id}
                  task={task}
                  deleteTask={deleteTask}
                />
              ),
          )}
          <NewTaskInputRow createTask={createTask} disabled={!project} />
        </TableBody>
      </Table>
    </div>
  );
}

export function TaskRow({
  task,
  deleteTask,
}: {
  task: Loaded<typeof Task> | undefined;
  deleteTask: (taskId: string) => void;
}) {
  return (
    <TableRow>
      <TableCell>
        <Checkbox
          className="mt-1"
          checked={task?.done}
          onCheckedChange={(checked) => {
            // Tick or untick the task
            // Task is also immutable, but this will update all queries
            // that include this task as a reference
            if (task) task.$jazz.set("done", !!checked);
          }}
        />
      </TableCell>
      <TableCell>
        <div className="flex flex-row justify-between items-center gap-2">
          {task?.text ? (
            <span className={task?.done ? "line-through" : ""}>
              {task.text}
            </span>
          ) : (
            <Skeleton className="mt-1 w-[200px] h-[1em] rounded-full" />
          )}
          {
            // Here we see for the first time how we can access edit history
            // for a CoValue, and use it to display who created the task.
            task?.$jazz.getEdits().text?.by?.profile?.name ? (
              <span
                className="rounded-full py-0.5 px-2 text-xs"
                style={uniqueColoring(
                  task.$jazz.getEdits().text?.by?.$jazz.id ?? "",
                )}
              >
                {task.$jazz.getEdits().text?.by?.profile?.name}
              </span>
            ) : (
              <Skeleton className="mt-1 w-[50px] h-[1em] rounded-full" />
            )
          }
        </div>
      </TableCell>
      <TableCell>
        <Button
          variant="outline"
          size="sm"
          onClick={() => deleteTask(task?.$jazz.id ?? "")}
          disabled={!task}
          className="border-red-300 text-red-600 hover:bg-red-50 hover:text-red-700 hover:border-red-400"
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      </TableCell>
    </TableRow>
  );
}

/** Walkthrough: This is the end of the walkthrough so far! */

function NewTaskInputRow({
  createTask,
  disabled,
}: {
  createTask: (text: string) => void;
  disabled: boolean;
}) {
  return (
    <TableRow>
      <TableCell>
        <Checkbox className="mt-1" disabled />
      </TableCell>
      <TableCell>
        <SubmittableInput
          onSubmit={(taskText) => createTask(taskText)}
          label="Add"
          placeholder="New task"
          disabled={disabled}
        />
      </TableCell>
      <TableCell>{/* Empty cell for delete button column */}</TableCell>
    </TableRow>
  );
}

function uniqueColoring(seed: string) {
  const darkMode = window.matchMedia("(prefers-color-scheme: dark)").matches;

  return {
    color: uniqolor(seed, { lightness: darkMode ? 80 : 20 }).color,
    background: uniqolor(seed, { lightness: darkMode ? 20 : 80 }).color,
  };
}
