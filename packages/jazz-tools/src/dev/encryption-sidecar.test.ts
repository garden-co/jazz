import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  ENCRYPTION_SIDECAR_FORMAT,
  encryptionEntriesText,
  parseEncryptionEntries,
} from "./migrations.js";

// `migrations/snapshots/<snapshot>.encryption.json` is a committed project
// file: a version's encryption entries, as `jazz.app.json` writes them,
// including a widened enum's `sealedType`. This fixture pins format 1.
const FIXTURE = join(
  import.meta.dirname,
  "fixtures",
  "encryption-sidecar",
  "format-1.encryption.json",
);
const fixture = readFileSync(FIXTURE, "utf8");
const entries = {
  notes: {
    space: "id",
    scope: "notes",
    columns: {
      title: "string",
      status: {
        type: { enum: ["draft", "sent"], optional: true },
        sealedType: { enum: ["draft"], optional: true },
        default: "draft",
      },
    },
  },
};

describe("snapshot encryption sidecars", () => {
  it("writes format 1 byte for byte", () => {
    expect(ENCRYPTION_SIDECAR_FORMAT).toBe(1);
    expect(encryptionEntriesText(entries)).toBe(fixture);
  });

  it("reads format 1, with its sealed enum type", () => {
    expect(parseEncryptionEntries("format-1.encryption.json", fixture)).toEqual(entries);
  });

  it("reads a sidecar written before the format was stamped as format 1", () => {
    const unstamped = JSON.stringify({ tables: entries });
    expect(parseEncryptionEntries("unstamped.encryption.json", unstamped)).toEqual(entries);
  });

  it("refuses a newer format instead of dropping what it doesn't know", () => {
    const newer = JSON.stringify({ format: 2, tables: entries });
    expect(() => parseEncryptionEntries("newer.encryption.json", newer)).toThrow(
      "migrations/snapshots/newer.encryption.json: format 2 is newer than this jazz-tools reads (format 1). Upgrade jazz-tools.",
    );
  });
});
