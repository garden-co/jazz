import { useAll } from '@jazz/react';
import { app } from '../generated/client';
import type { Task } from '../generated/types';

export function TaskList() {
  // Subscribe to all tasks - updates automatically when data changes
  const [tasks, loading, mutate] = useAll(app.tasks);

  if (loading) {
    return <div>Loading tasks...</div>;
  }

  const toggleTask = (task: Task) => {
    mutate.update(task.id, { completed: !task.completed });
  };

  const deleteTask = (taskId: string) => {
    mutate.delete(taskId);
  };

  return (
    <ul className="space-y-2">
      {tasks.map((task) => (
        <li
          key={task.id}
          className="flex items-center gap-2 p-2 border rounded"
        >
          <input
            type="checkbox"
            checked={task.completed}
            onChange={() => toggleTask(task)}
          />
          <span className={task.completed ? 'line-through' : ''}>
            {task.title}
          </span>
          <span className="text-sm text-gray-500">[{task.priority}]</span>
          <button
            onClick={() => deleteTask(task.id)}
            className="ml-auto text-red-500"
          >
            Delete
          </button>
        </li>
      ))}
    </ul>
  );
}
