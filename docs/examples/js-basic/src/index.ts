// Basic Jazz client usage without React

import type { WasmDatabaseLike } from '@jazz/client';

// Initialize Jazz and create some tasks
async function main() {
  // Load WASM module
  const wasm = await import('groove-wasm');
  await wasm.default();

  // Create database with schema
  const db: WasmDatabaseLike = wasm.Database.new();
  db.execute(`
    CREATE TABLE Tasks (
      title STRING NOT NULL,
      completed BOOLEAN NOT NULL
    )
  `);

  // Insert a task
  db.execute(
    'INSERT INTO Tasks (id, title, completed) VALUES (?, ?, ?)',
    ['task1', 'Learn Jazz', false]
  );

  // Query tasks
  const tasks = db.query('SELECT * FROM Tasks');
  console.log('Tasks:', tasks);

  // Update a task
  db.execute(
    'UPDATE Tasks SET completed = ? WHERE id = ?',
    [true, 'task1']
  );

  // Subscribe to changes
  db.subscribe('SELECT * FROM Tasks', [], (rows) => {
    console.log('Tasks updated:', rows);
  });
}

main();
