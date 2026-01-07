// Generated from SQL schema by @jazz/schema
// This is a simplified version for documentation examples

import type { Task, TaskInsert, TaskFilter, ObjectId } from './types.js';

// Stub types for documentation - in real usage these come from @jazz/client
type Unsubscribe = () => void;
type WasmDatabaseLike = unknown;

interface TasksDescriptor {
  create(db: WasmDatabaseLike, data: TaskInsert): ObjectId;
  update(db: WasmDatabaseLike, id: ObjectId, data: Partial<TaskInsert>): void;
  delete(db: WasmDatabaseLike, id: ObjectId): void;
  subscribeAll(db: WasmDatabaseLike, callback: (rows: Task[]) => void): Unsubscribe;
  subscribe(db: WasmDatabaseLike, id: ObjectId, callback: (row: Task | null) => void): Unsubscribe;
  where(filter: TaskFilter): TasksDescriptor;
}

export const app = {
  tasks: {} as TasksDescriptor,
};

export type { Task, TaskInsert, TaskFilter, ObjectId };
