import { useAll } from '@jazz/react';
import { app } from '../generated/client';

// Custom hook demonstrating filtered queries with live updates
export function useFilteredTasks(showCompleted: boolean) {
  // The query updates incrementally when tasks change
  // Only affected rows trigger recomputation
  const query = showCompleted
    ? app.tasks
    : app.tasks.where({ completed: false });

  const [tasks, loading] = useAll(query);

  return { tasks, loading };
}
