import { useOne } from '@jazz/react';
import { app } from '../generated/client';

interface TaskDetailProps {
  taskId: string;
}

export function TaskDetail({ taskId }: TaskDetailProps) {
  // Subscribe to a single task by ID - updates when that task changes
  const [task, loading, mutate] = useOne(app.tasks, taskId);

  if (loading) {
    return <div>Loading task...</div>;
  }

  if (!task) {
    return <div>Task not found</div>;
  }

  return (
    <div className="p-4 border rounded">
      <h2 className="text-xl font-bold">{task.title}</h2>
      {task.description && (
        <p className="text-gray-600 mt-2">{task.description}</p>
      )}
      <div className="mt-4 flex gap-2">
        <button
          onClick={() => mutate.update({ completed: !task.completed })}
          className="px-3 py-1 bg-blue-500 text-white rounded"
        >
          {task.completed ? 'Mark Incomplete' : 'Mark Complete'}
        </button>
        <button
          onClick={() => mutate.delete()}
          className="px-3 py-1 bg-red-500 text-white rounded"
        >
          Delete
        </button>
      </div>
    </div>
  );
}
