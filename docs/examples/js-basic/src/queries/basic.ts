// Basic query examples

import type { WasmDatabaseLike } from '@jazz/client';

interface Task {
  id: string;
  title: string;
  completed: boolean;
  priority: string;
}

export async function basicQueryExamples(db: WasmDatabaseLike) {
  // Select all tasks
  const allTasks: Task[] = db.query('SELECT * FROM Tasks');

  // Select with WHERE clause
  const activeTasks: Task[] = db.query(
    'SELECT * FROM Tasks WHERE completed = ?',
    [false]
  );

  // Select with ORDER BY
  const sortedTasks: Task[] = db.query(
    'SELECT * FROM Tasks ORDER BY createdAt DESC'
  );

  // Select with LIMIT
  const recentTasks: Task[] = db.query(
    'SELECT * FROM Tasks ORDER BY createdAt DESC LIMIT ?',
    [10]
  );

  // Aggregate query
  const counts = db.query(`
    SELECT
      completed,
      COUNT(*) as count
    FROM Tasks
    GROUP BY completed
  `);

  return { allTasks, activeTasks, sortedTasks, recentTasks, counts };
}
