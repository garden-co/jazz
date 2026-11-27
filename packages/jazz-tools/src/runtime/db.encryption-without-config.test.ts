import { expect, it } from "vitest";
import { EncryptionNotDeclaredError } from "../encryption/index.js";
import { Db } from "./db.js";

// A Db with no config of its own (the backend's early db is one before its
// store opens) still has a `db.encryption`: the one that says the app
// declares no encryption, not a TypeError from reading its config.
it("answers db.encryption of a Db without a config as not declared", () => {
  const bare = Object.create(Db.prototype) as Db;
  let handle: Db["encryption"] | undefined;
  expect(() => (handle = bare.encryption)).not.toThrow();
  expect(() => handle!.deviceId).toThrow(EncryptionNotDeclaredError);
});
