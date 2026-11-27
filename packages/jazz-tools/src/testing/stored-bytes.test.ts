import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { storedBytes } from "./stored-bytes.js";

vi.mock(import("node:fs/promises"), async (importOriginal) => {
  const fs = await importOriginal();
  return { ...fs, readFile: vi.fn(fs.readFile) as typeof fs.readFile };
});

it("fails a storage scan when a file cannot be read", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-storage-scan-"));
  try {
    await writeFile(join(dir, "store"), "stored bytes");
    await expect(storedBytes(dir)).resolves.toEqual(Buffer.from("stored bytes"));
    const readError = Object.assign(new Error("cannot read the store file"), { code: "EACCES" });
    vi.mocked(readFile).mockRejectedValueOnce(readError);
    await expect(storedBytes(dir)).rejects.toBe(readError);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

it("reads a file store and its journal when the WAL is absent", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-storage-scan-"));
  const store = join(dir, "store");
  try {
    await writeFile(store, "database");
    await writeFile(store + "-journal", "journal");
    await expect(storedBytes(store)).resolves.toEqual(Buffer.from("databasejournal"));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

it("fails a storage scan when a SQLite sidecar cannot be read", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jazz-storage-scan-"));
  const store = join(dir, "store");
  const wal = store + "-wal";
  const readError = Object.assign(new Error("cannot read the WAL"), { code: "EACCES" });
  try {
    await writeFile(store, "database");
    await writeFile(wal, "WAL");
    const fs = await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises");
    vi.mocked(readFile).mockImplementation((path, options) =>
      path === wal ? Promise.reject(readError) : fs.readFile(path, options),
    );
    await expect(storedBytes(store)).rejects.toBe(readError);
  } finally {
    vi.mocked(readFile).mockReset();
    await rm(dir, { recursive: true, force: true });
  }
});
