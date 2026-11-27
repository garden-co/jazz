import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { encryptionEntriesText, parseEncryptionEntries } from "./migrations.js";

// `migrations/snapshots/<snapshot>.encryption.json` is a committed project
// file: a version's encryption entries, as `jazz.app.json` writes them,
// including a widened enum's `sealedType`. This fixture pins its shape.
const FIXTURE = join(
  import.meta.dirname,
  "fixtures",
  "encryption-sidecar",
  "sealed-type.encryption.json",
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
  it("writes a sealed enum type byte for byte as the fixture", () => {
    expect(encryptionEntriesText(entries)).toBe(fixture);
  });

  it("reads the fixture back, with its sealed enum type", () => {
    expect(parseEncryptionEntries("sealed-type.encryption.json", fixture)).toEqual(entries);
  });
});
