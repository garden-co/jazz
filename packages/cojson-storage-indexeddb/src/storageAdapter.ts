import { AsyncStorageAdapter } from "cojson-storage";
import { IDBClient } from "./idbClient.js";
import { openDatabase } from "./idbNode.js";

export async function loadIDBStorageAdapter() {
  return new AsyncStorageAdapter(new IDBClient(await openDatabase()));
}
