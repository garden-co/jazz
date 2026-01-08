// This file demonstrates the generated types pattern
// These types are similar to what @jazz/schema generates from SQL

//#region generated-types
// Auto-generated from schema.sql by @jazz/schema

export interface User {
  id: string;
  name: string;
  email: string;
}

export interface Task {
  id: string;
  title: string;
  description: string | null;
  completed: boolean;
  user: string; // ObjectId reference to User
}
//#endregion

//#region task-with-user
// When using .with({ user: true }), the type expands:
export interface TaskWithUser {
  id: string;
  title: string;
  description: string | null;
  completed: boolean;
  user: User; // Full User object instead of just ObjectId
}
//#endregion
