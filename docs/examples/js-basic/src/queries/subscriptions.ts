// Query subscription examples

import type { WasmDatabaseLike, Unsubscribe } from '@jazz/client';

interface Task {
  id: string;
  title: string;
  completed: boolean;
}

// Subscribe to a query - callback fires on every change
export function subscribeToActiveTasks(
  db: WasmDatabaseLike,
  onUpdate: (tasks: Task[]) => void
): Unsubscribe {
  return db.subscribe(
    'SELECT * FROM Tasks WHERE completed = ?',
    [false],
    onUpdate
  );
}

// Example: Real-time task counter
export function createTaskCounter(db: WasmDatabaseLike) {
  let count = 0;

  const unsubscribe = db.subscribe(
    'SELECT COUNT(*) as count FROM Tasks WHERE completed = false',
    [],
    (result) => {
      count = result[0]?.count ?? 0;
      console.log(`Active tasks: ${count}`);
    }
  );

  return {
    getCount: () => count,
    stop: unsubscribe,
  };
}

// Example: Filtered subscription
export function subscribeToHighPriorityTasks(
  db: WasmDatabaseLike,
  onUpdate: (tasks: Task[]) => void
): Unsubscribe {
  return db.subscribe(
    `SELECT * FROM Tasks
     WHERE completed = false AND priority = ?
     ORDER BY createdAt DESC`,
    ['high'],
    onUpdate
  );
}
