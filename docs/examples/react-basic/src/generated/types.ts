// Generated from SQL schema by @jazz/schema
// This is a simplified version for documentation examples

export type ObjectId = string;

export interface GrooveRow {
  id: ObjectId;
}

export interface Task extends GrooveRow {
  title: string;
  description: string | null;
  completed: boolean;
  priority: string;
  createdAt: bigint;
}

export interface TaskInsert {
  title: string;
  description?: string | null;
  completed: boolean;
  priority: string;
  createdAt: bigint;
}

export interface TaskFilter {
  id?: string;
  title?: string;
  completed?: boolean;
  priority?: string;
}
