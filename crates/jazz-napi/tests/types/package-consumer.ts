import { NapiDb, type JsonValue } from "jazz-napi";

const branch: JsonValue = {
  name: "draft",
  parents: ["main"],
  archived: false,
};

declare const db: NapiDb;
declare const encodedRow: Uint8Array;

db.insertEncoded("documents", encodedRow, { branch });
const closeResult: Promise<undefined> = db.close();
void closeResult;
