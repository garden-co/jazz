// Transaction examples for atomic operations

import type { WasmDatabaseLike } from '@jazz/client';

// Multiple related operations that should succeed or fail together
export async function moveTaskToProject(
  db: WasmDatabaseLike,
  taskId: string,
  newProjectId: string
) {
  // In Jazz, each mutation is automatically wrapped in a transaction
  // For multiple operations, execute them in sequence

  // Update the task's project
  db.execute(
    'UPDATE Tasks SET project = ?, updatedAt = ? WHERE id = ?',
    [newProjectId, Date.now(), taskId]
  );

  // Log the move in an activity table
  db.execute(
    `INSERT INTO Activities (id, type, taskId, projectId, timestamp)
     VALUES (?, ?, ?, ?, ?)`,
    [crypto.randomUUID(), 'task_moved', taskId, newProjectId, Date.now()]
  );
}

// Batch create multiple tasks
export function createMultipleTasks(
  db: WasmDatabaseLike,
  titles: string[]
) {
  const ids: string[] = [];

  for (const title of titles) {
    const id = crypto.randomUUID();
    db.execute(
      `INSERT INTO Tasks (id, title, completed, priority, createdAt)
       VALUES (?, ?, ?, ?, ?)`,
      [id, title, false, 'medium', Date.now()]
    );
    ids.push(id);
  }

  return ids;
}
