import { SyncStorageAdapter } from "cojson-storage";
import { SQLiteClient } from "./sqliteClient.js";
import { openDatabase } from "./sqliteNode.js";

export function loadSQLiteStorageAdapter(filename: string) {
  const db = openDatabase(filename);
  return new SyncStorageAdapter(new SQLiteClient(db));
}
