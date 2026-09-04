import { NapiDb, type JsonValue, type UpsertOptions } from "jazz-napi";

const branch: JsonValue = {
  name: "draft",
  parents: ["main"],
  archived: false,
};

declare const db: NapiDb;
declare const encodedRow: Uint8Array;

db.insertEncoded("documents", encodedRow, { branch });
const canonicalBranchUpsert: UpsertOptions = { head: branch };
const removedBranchUpsert: UpsertOptions = {
  // @ts-expect-error `branch` is not an upsert selector; use `head`.
  branch,
};
const closeResult: Promise<undefined> = db.close();
void closeResult;
void canonicalBranchUpsert;
void removedBranchUpsert;
