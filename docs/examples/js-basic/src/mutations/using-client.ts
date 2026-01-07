// Mutation examples using the generated client

import type { WasmDatabaseLike } from '@jazz/client';

interface TaskInsert {
  title: string;
  completed: boolean;
  priority: string;
  createdAt: bigint;
}

// In real usage, these would come from generated client
// import { app } from '../generated/client';

export function mutationExamples(db: WasmDatabaseLike) {
  // Create a new task using raw SQL
  const taskId = db.execute(
    `INSERT INTO Tasks (id, title, completed, priority, createdAt)
     VALUES (?, ?, ?, ?, ?)`,
    [crypto.randomUUID(), 'New task', false, 'medium', Date.now()]
  );

  // Update a task
  db.execute(
    'UPDATE Tasks SET completed = ? WHERE id = ?',
    [true, taskId]
  );

  // Delete a task
  db.execute('DELETE FROM Tasks WHERE id = ?', [taskId]);

  // Batch updates
  db.execute(
    'UPDATE Tasks SET completed = ? WHERE priority = ?',
    [true, 'low']
  );
}

// Using the generated type-safe client (preferred)
// export function typeSafeMutations(db: WasmDatabaseLike) {
//   const { tasks } = createDatabase(db);
//
//   // Create with full type safety
//   const id = tasks.create({
//     title: 'New task',
//     completed: false,
//     priority: 'medium',
//     createdAt: BigInt(Date.now()),
//   });
//
//   // Update with type checking
//   tasks.update(id, { completed: true });
//
//   // Delete
//   tasks.delete(id);
// }
